# Delivery Peak Experiment Plan

Goal: recover more priority deliveries during peak request hours while preserving
staking liveness. This document is a runbook only. Do not enable scanner,
fallback, parallel nonce, or batching from this plan without an explicit operator
decision.

## Current Safe Baseline

- `parallel_nonce_enabled=false`
- `queue_scanner_enabled=false`
- `fallback_mode_enabled=false`
- one on-chain request per Safe transaction
- delivery loop wakes immediately after execution
- serial Safe path prepares IPFS/payload data before taking the Safe lock

## Metrics To Capture

Use `/api/metrics/live`, `/api/metrics/events`, database aggregates, and logs.

- delivered/minute and received/minute
- max and p95 executed queue age
- `avg_delivery_age_seconds` and `p95_delivery_age_seconds`
- `avg_delivery_prep_seconds` and `p95_delivery_prep_seconds`
- `avg_safe_lock_wait_seconds` and `p95_safe_lock_wait_seconds`
- `avg_safe_submit_seconds` and `p95_safe_submit_seconds`
- `on_chain_timeout`, `on_chain_unavailable`, and DB/period `timed_out`
- `/api/metrics/live` `live.mech_late_delivery_count`
- Safe nonce progress, GS013/GS026, RPC 429/rate-limit errors
- staking epoch progress: required deliveries, current deliveries, rewards

## Stop Conditions

Stop any experiment and return to the baseline config if one of these happens:

- any increase in `/api/metrics/live` `live.mech_late_delivery_count`
- `on_chain_timeout >= 2` in 15 minutes
- DB/period `timed_out >= 2` in 15 minutes
- p95 delivery age above 120 seconds for two consecutive samples
- max executed queue age above 180 seconds
- delivered/minute below received/minute for 10 minutes
- Safe nonce stuck for more than 180 seconds while submissions are active
- GS026 or nonce-gap alerts appear
- RPC 429/rate-limit errors above 1 percent of calls

## Stage 0: Observe Baseline

Run at least one peak window with the current baseline.

Record:

- number of priority requests delivered by us
- number of priority requests delivered by another mech
- gross lost revenue: `lost_requests * delivery_rate`
- OLAS staking rewards expected for the same period
- p95 delivery age, prep, Safe lock wait, submit time

Decision:

- if submit time dominates, focus RPC/provider and Safe transaction inclusion
- if lock wait dominates, investigate concurrent Safe users
- if prep dominates, investigate IPFS latency or payload build latency
- if age is high but prep/lock/submit are low, investigate listener/execution backlog

## Stage 1: RPC Read-Only Capacity Test

Do not change delivery behavior.

Run read-only probes against candidate RPC providers:

- `eth_blockNumber`
- small `eth_getLogs` windows
- `getRequestStatus`
- receipt lookups

Pass criteria:

- p95 read latency below 2x baseline
- 429/rate-limit below 1 percent
- no provider rotation loop

## Stage 2: Own Queue Scanner Canary

Only consider this after Stage 0 and Stage 1.

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

Start with the values above for 30 minutes. The own-queue scanner walks own
queue pages until drained, so the practical RPC ceiling must be monitored from
logs. Estimate each cycle as:

```text
pages = ceil(queue_depth / queue_scanner_page_size)
rough_rpc_calls = pages + candidate_requests + block/log/status lookups
```

For the first canary, stop before the cycle exceeds 5 own-queue pages or roughly
75 scanner RPC calls. With `page_size=10`, that means no more than about 50
candidate requests before manual review. Raise interval/page budgets only after
analysing actual RPC logs.

Do not combine this with fallback or parallel nonce in the same canary.

Pass criteria:

- scanner finds missed own requests without duplicate execution
- RPC budget remains healthy
- queue age does not increase

Scanner-specific stop conditions:

- scanner cycle takes longer than half of `queue_scanner_interval_seconds`
- more than 5 own-queue pages in a cycle
- more than 75 scanner RPC calls in a cycle
- any RPC 429/rate-limit event
- queue age p95 rises versus baseline during scanner cycles

## Stage 3: Parallel Nonce Staging Only

Do not run in production until staging/fork proves:

- cancellation does not leak `_in_flight`
- GS026/nonce-gap recovery falls back cleanly
- p95 delivery age improves enough to justify risk
- stop conditions are automated or actively monitored

Production canary, if approved later, should be short and off-peak first.

## Stage 4: Batch Economics

Batch delivery trades throughput for fewer Safe nonces. This can harm staking
liveness if activity checks depend on delivery/nonce deltas.

Only consider batching if measured daily lost request revenue is greater than
expected daily staking rewards after gas and risk margin.

Formula:

```text
lost_request_revenue = lost_requests * delivery_rate
staking_value = daily_OLAS_rewards * OLAS_price
batching_candidate = lost_request_revenue > staking_value * risk_margin
```

Use a risk margin of at least 2x before sacrificing staking liveness.

## Immediate Next Operational Move

After deploying the current code in a controlled window, do not enable any new
feature. Observe one peak period and compare:

- before/after `p95_delivery_age_seconds`
- before/after priority requests delivered by other mechs
- prep vs lock-wait vs submit contribution

Only choose the next lever after those numbers identify the bottleneck.
