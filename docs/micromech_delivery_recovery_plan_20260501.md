# Micromech Delivery Recovery Plan — 2026-05-01

This document records what happened, what we changed, why we changed it, and
what we should do next. It is intentionally conservative: no feature should be
enabled from this document without a separate operator decision.

## Context

Micromech earns delivery revenue by answering marketplace requests and delivering
the result on-chain. It also relies on OLAS staking liveness, where the current
safe assumption is one successful on-chain delivery per Safe transaction nonce.

The business problem is peak-hour loss: during high request windows, some
requests where our mech is the priority mech are eventually delivered by other
mechs. That means users still get answers, but we lose delivery revenue and
potentially harm karma/staking performance.

## Prior Incidents We Must Not Repeat

### 2026-04-24: Parallel Safe Nonce Collapse

Source: `/home/david/Descargas/postmortem-micromech-20260424.md`

The 2026-04-24 incident was a severe delivery collapse. Micromech delivered only
440 of roughly 2879 `echo` requests that day, about 15 percent success, with an
estimated loss of about 85 percent of daily income.

Root cause:

- `parallel_nonce_enabled=true` was enabled in production.
- Three Safe delivery workers submitted transactions using pre-assigned nonces.
- When nonce `N` was not mined quickly, nonce `N+1` and `N+2` simulated against
  stale on-chain state and hit GS026.
- iwa retried those GS026 nonce races aggressively.
- Workers became blocked for long periods, delivery throughput fell below
  request arrival rate, and requests aged past the 300 second response window.

Observed impact:

- 2439 failed deliveries on 2026-04-24.
- 2404 were `pre_delivery_expired`.
- 35 were `on_chain_timeout`.
- Several hours had zero successful deliveries.

Lessons carried forward:

- Do not enable parallel nonce mode in production without staging stress tests.
- Nonce retry can amplify failure when several Safe transactions race together.
- Queue age and timeout metrics matter more than daily revenue after the fact.
- Safe nonce stuck alerts need to be treated as critical, not advisory.

### 2026-04-27: Fallback Mode Overload

Source: `/home/david/Descargas/informe_fallback_mode_incidente.md`

The 2026-04-27 incident was caused by enabling `fallback_mode_enabled=true` in
production after v0.0.40.

Root cause:

- Fallback mode made micromech listen to all `MarketplaceRequest` events, not
  only requests where our mech was the priority mech.
- Production volume increased roughly 3-4x.
- Delivery queue pressure increased, Safe errors increased, and deliveries
  started failing.

Observed impact:

- 127 confirmed failed deliveries in the database between 08:00 and 10:00 local.
- Failure rate rose from 0 percent before fallback to 21 percent, then 41 percent.
- Logs showed 252 GS013 warnings, 18 GS026 errors, and 80 IPFS warnings.

Lessons carried forward:

- Fallback mode is not a free revenue switch.
- It must not be enabled together with other risky delivery features.
- Any fallback experiment needs strict RPC and page/candidate budgets.
- Production must alert loudly if `fallback_mode_enabled=true`.

## 2026-04-30 / 2026-05-01 Peak Loss Investigation

Telegram alerts reported about 110 actionable delivery issues per hour around
the 2026-04-30 peak window.

We verified on-chain samples from the last 12 hours. The important result:

- The requests were not left unanswered.
- Other mechs delivered them.
- Our mech attempted delivery too late, often in the same block or one block
  after the other mech.
- The old metric classification made these look like simple timeouts, but the
  actual business loss was priority requests being captured by competitors.

This means the next goal is not "recover old open requests"; old requests should
still be handled separately by the backup mech flow. The goal is reducing
execution-to-delivery latency and measuring exactly where the delay occurs.

## Decisions Made

We deliberately did not enable:

- `parallel_nonce_enabled`
- `queue_scanner_enabled`
- `fallback_mode_enabled`
- batch delivery

Reason:

- Parallel nonce already caused a major collapse.
- Fallback already caused a major overload.
- Batch delivery may improve throughput but can reduce Safe nonce count relative
  to delivery count, which can threaten OLAS staking liveness.
- We did not yet have enough timing metrics to know the actual bottleneck.

The chosen path was to make the current conservative serial mode faster and more
observable.

## Changes Implemented

### Commit `329784c`: Classify Marketplace Delivery Rejections

Purpose:

- Distinguish late rejected deliveries from requests already finalized on-chain.

Effect:

- If the marketplace status is delivered or unavailable, classify as
  `on_chain_unavailable`.
- If the marketplace status is expired, preserve `on_chain_timeout`.
- If status lookup fails, classify as `on_chain_rejected: status_unknown`.

Why:

- Alerts should not treat every false delivery flag as the same failure.
- We need to know whether we were late, beaten by another mech, or facing an
  unknown chain/RPC issue.

### Commit `bc8c018`: Wake Delivery After Request Execution

Purpose:

- Reduce idle delay between tool execution finishing and the next delivery tick.

Before:

- Delivery loop waited up to `delivery_interval` before noticing newly executed
  requests.

After:

- `MechServer._execute_and_cleanup()` wakes the per-chain `DeliveryManager`
  immediately after execution.
- `DeliveryManager.stop()` also wakes a sleeping loop for cleaner shutdown.

Expected benefit:

- Lower execution-to-delivery latency by up to one delivery interval.

### Commit `6943b5f`: Prepare Serial Safe Deliveries Before Locking

Purpose:

- Reduce time spent holding the Safe lock.

Before:

- The serial Safe path prepared IPFS/payload data while holding the Safe lock.

After:

- On-chain payload preparation happens before `get_safe_lock`.
- The Safe lock is held only for Safe transaction submission.
- Cancellation paths clean all selected records from `_in_flight`.

Expected benefit:

- IPFS latency no longer blocks other Safe users.
- Serial mode remains one request per Safe transaction.
- Staking liveness assumptions remain unchanged.

### Commit `5aab545`: Add Delivery Timing Observability

Purpose:

- Measure the actual delivery bottleneck instead of guessing.

New live metrics:

- `avg_delivery_age_seconds`
- `p95_delivery_age_seconds`
- `avg_delivery_prep_seconds`
- `p95_delivery_prep_seconds`
- `avg_safe_lock_wait_seconds`
- `p95_safe_lock_wait_seconds`
- `avg_safe_submit_seconds`
- `p95_safe_submit_seconds`

New event:

- `delivery_timing` in `/api/metrics/events`

Why:

- If submit time dominates, the next lever is RPC/provider/tx inclusion.
- If prep time dominates, the next lever is IPFS/payload preparation.
- If lock wait dominates, the next lever is Safe contention.
- If all are low but requests are still lost, the issue is likely listener lag,
  execution backlog, or marketplace competition timing.

### Commit `35d9046`: Document Delivery Peak Experiment Plan

Created:

- `docs/delivery_peak_experiment_plan.md`

Purpose:

- Define a staged plan for measuring and experimenting without accidentally
  enabling risky features.

## Validation Performed

Static checks:

```bash
uv run ruff check src/ tests/
uv run mypy src/
```

Result:

- Ruff passed.
- Mypy passed.

Focused delivery and metrics tests:

```bash
uv run pytest tests/unit/test_metrics.py \
  tests/unit/test_delivery.py \
  tests/unit/test_delivery_extra.py \
  tests/unit/test_concurrent_delivery.py \
  tests/unit/test_server.py -q --no-cov
```

Result:

- 238 passed.

Unit suite:

```bash
uv run pytest tests/unit/ -q --no-cov
```

Result:

- 2084 passed.

Full suite with coverage:

```bash
uv run pytest tests/
```

Result:

- 2150 passed.
- 7 skipped.
- Coverage: 91.64 percent, above the 90 percent gate.
- 5 failures occurred in the first run.

Failure analysis:

- One Anvil lifecycle E2E failed due to external fork RPC HTTP 429.
- Four listener/polling tests failed in the full run but passed isolated.
- The Anvil lifecycle E2E passed when repeated isolated.
- The Safe throughput benchmark failed in the integration folder due to external
  fork RPC HTTP 429, then passed when repeated isolated.

Conclusion:

- Unit and focused delivery coverage is strong.
- The remaining instability is integration-test infrastructure/RPC rate limiting,
  not a reproduced code failure in the delivery changes.
- Before a high-confidence release, repeat integration tests with a dedicated or
  less rate-limited fork RPC.

## Current Safe Baseline

Required config:

```yaml
parallel_nonce_enabled: false
queue_scanner_enabled: false
fallback_mode_enabled: false
```

Required behavior:

- one on-chain request per Safe transaction
- serial Safe submissions
- no fallback harvesting
- no broad marketplace scanning
- no batch delivery beyond the current batch size of 1

## Deployment Plan

1. Deploy the current code only.
2. Do not change production feature flags.
3. Restart micromech in a quiet window.
4. Confirm config after restart:

```bash
parallel_nonce_enabled=false
queue_scanner_enabled=false
fallback_mode_enabled=false
```

5. Confirm `/health` is OK.
6. Confirm `/api/metrics/live` exposes the new timing fields.
7. Observe one full peak window before making any further decision.

## Metrics To Watch During The First Peak

Use `/api/metrics/live`, `/api/metrics/events`, DB aggregates, logs, and on-chain
checks.

Core metrics:

- delivered per minute
- received per minute
- max executed queue age
- p95 executed queue age
- `p95_delivery_age_seconds`
- `p95_delivery_prep_seconds`
- `p95_safe_lock_wait_seconds`
- `p95_safe_submit_seconds`
- `live.mech_late_delivery_count`
- `on_chain_timeout`
- DB/period `timed_out`
- requests where `priorityMech` is ours but `deliveryMech` is another mech
- Safe nonce progress
- GS013/GS026 count
- RPC 429/rate-limit count

Interpretation:

- High `safe_submit_seconds`: RPC, gas, Safe service, or tx inclusion bottleneck.
- High `safe_lock_wait_seconds`: Safe contention.
- High `delivery_prep_seconds`: IPFS or payload preparation bottleneck.
- High `delivery_age_seconds` but low prep/lock/submit: listener lag, execution
  backlog, or wake/queue scheduling issue.

## Stop Conditions

If any of these happen after deploy, revert to the previous known-good image or
stop and diagnose:

- `live.mech_late_delivery_count` increases unexpectedly.
- `on_chain_timeout >= 2` in 15 minutes.
- DB/period `timed_out >= 2` in 15 minutes.
- p95 delivery age above 120 seconds for two consecutive samples.
- max executed queue age above 180 seconds.
- delivered/minute below received/minute for 10 minutes.
- Safe nonce stuck for more than 180 seconds while submissions are active.
- any GS026 or nonce-gap alert.
- RPC 429/rate-limit errors above 1 percent of calls.

## Next Decision Tree

After one measured peak:

### If submit time dominates

Next action:

- test a better/dedicated RPC provider
- separate read RPC and send/receipt RPC if useful
- monitor receipt wait and tx inclusion latency

Do not:

- enable parallel nonce as the first response

### If prep time dominates

Next action:

- inspect IPFS push latency
- add IPFS timeout/fallback metrics
- consider local IPFS/provider change

Do not:

- hold the Safe lock during prep again

### If lock wait dominates

Next action:

- identify other Safe users such as withdrawals/checkpoints
- schedule or serialize non-delivery Safe operations away from peaks

Do not:

- increase Safe concurrency without staging tests

### If listener lag or missed events dominate

Next action:

- run Stage 1 RPC read-only capacity tests
- then consider own-queue scanner canary

Do not:

- enable fallback mode

### If lost request revenue exceeds staking value

Next action:

- model batch delivery economics
- compare lost delivery revenue with OLAS staking rewards and risk margin

Formula:

```text
lost_request_revenue = lost_requests * delivery_rate
staking_value = daily_OLAS_rewards * OLAS_price
batching_candidate = lost_request_revenue > staking_value * risk_margin
```

Use a risk margin of at least 2x before sacrificing staking liveness.

## Scanner Canary, If Needed Later

Only consider this after baseline observation and RPC read-only tests.

Candidate config:

```yaml
queue_scanner_enabled: true
fallback_mode_enabled: false
parallel_nonce_enabled: false
queue_scanner_interval_seconds: 600
queue_scanner_page_size: 10
queue_scanner_fallback_pages_per_cycle: 1
queue_scanner_event_lookback_blocks: 720
```

First canary limits:

- 30 minutes.
- Stop before more than 5 own-queue pages in one cycle.
- Stop before roughly 75 scanner RPC calls in one cycle.
- With page size 10, that means around 50 candidate requests before manual review.

Scanner stop conditions:

- scanner cycle takes longer than half of `queue_scanner_interval_seconds`
- any RPC 429/rate-limit event
- queue age p95 rises versus baseline
- duplicate execution appears
- any timeout metric increases

## Parallel Nonce, If Reconsidered Later

Parallel nonce remains quarantined.

Minimum requirements before any production canary:

- staging/fork test with delayed nonce N mining
- test with RPC errors
- test with cancellation during prep and submit
- circuit breaker back to serial
- no `_in_flight` leaks
- no GS026 storm
- clear operator watching the canary

Production canary, if ever approved:

- off-peak first
- short duration
- fallback disabled
- scanner disabled
- immediate rollback on GS026, nonce gap, or queue-age growth

## Batch Delivery, If Reconsidered Later

Batching can recover throughput but may reduce Safe nonce count relative to
delivery count. That is dangerous if staking liveness depends on nonce/delivery
ratios.

Do not batch while staking economics are unclear.

Batching becomes a candidate only if:

- measured lost request revenue is consistently larger than expected OLAS staking
  rewards after gas and risk margin
- we can prove staking liveness remains safe, or we consciously accept losing
  staking rewards
- we test on a separate service or controlled fork first

## Immediate Next Step

Deploy the current code without enabling any feature flags. Observe the next peak
window and fill in:

```text
date:
peak window:
requests received:
requests delivered by us:
priority requests delivered by another mech:
lost request revenue:
p95 delivery age:
p95 prep:
p95 Safe lock wait:
p95 Safe submit:
timeouts:
GS013:
GS026:
RPC 429:
staking progress:
decision:
```

Only after that observation should we choose between RPC improvement, scanner
canary, IPFS work, Safe-operation scheduling, or a more aggressive economic
experiment.
