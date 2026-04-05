# micromech

Run your own [OLAS](https://olas.network/) mech and earn rewards. micromech answers on-chain AI requests using pluggable tools and stakes automatically to earn OLAS tokens.

## Install

You need [Docker](https://docs.docker.com/get-docker/) installed. Then run:

```bash
bash <(curl -sSL https://raw.githubusercontent.com/dvilelaf/micromech/main/scripts/quickstart.sh)
```

This creates a `micromech/` folder with everything you need.

## Setup

```bash
cd micromech
just up
```

Open **http://localhost:8000** in your browser. The setup wizard walks you through:

1. Create or unlock your wallet
2. Choose which chains to run on
3. Fund your wallet
4. Deploy your mech service
5. Start earning rewards

That's it. micromech handles the rest automatically: listening for requests, executing tools, delivering results, calling checkpoints, and claiming rewards.

## Remote Control (optional)

Control your mech from your phone via Telegram:

1. Create a bot with [@BotFather](https://t.me/BotFather) on Telegram
2. Get your chat ID from [@userinfobot](https://t.me/userinfobot)
3. Add both to `secrets.env`:
   ```
   telegram_token=your-bot-token
   telegram_chat_id=your-chat-id
   ```
4. Restart: `just down && just up`

Now you can use `/status`, `/claim`, `/logs`, `/settings` and more from Telegram.

## Day-to-day

| Command | What it does |
|---------|-------------|
| `just up` | Start micromech |
| `just down` | Stop micromech |
| `just logs` | View live logs |
| `just update` | Update to latest version |

micromech runs in the background and restarts automatically if your server reboots. Updates can also happen automatically if you enable auto-update in settings.

## What happens behind the scenes

1. Someone sends an AI request to the OLAS marketplace
2. micromech picks it up and runs the appropriate tool (LLM, prediction, etc.)
3. The result is delivered back on-chain
4. Your mech earns staking rewards for doing useful work

## Troubleshooting

```bash
just doctor    # Diagnose common issues
just logs      # Check what's happening
```

Dashboard: http://localhost:8000

## Advanced: Run from source

```bash
git clone https://github.com/dvilelaf/micromech
cd micromech
uv sync --all-extras
micromech init
micromech run
```

## License

Apache-2.0
