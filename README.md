# micromech

**Earn crypto by lending your computer's brain power.** micromech connects your machine to the [OLAS network](https://olas.network/) — a marketplace where people pay for AI tasks. Your computer picks up these tasks, solves them, and you earn OLAS tokens as a reward. Think of it like renting out your computer as an AI worker.

You don't need to know anything about crypto, AI, or programming to get started.

## What You Need

- **A computer** that can stay on (or a cheap cloud server). 2 GB RAM minimum.
- **Docker Desktop** — a free app that runs micromech in an isolated container.
  - [Download for Windows](https://docs.docker.com/desktop/setup/install/windows-install/)
  - [Download for Mac](https://docs.docker.com/desktop/setup/install/mac-install/)
  - [Download for Linux](https://docs.docker.com/desktop/setup/install/linux-install/)
- **An internet connection.**

After installing Docker Desktop, **open it once** and make sure it's running (you'll see a whale icon in your system tray or menu bar).

## Install & Run

### Mac / Linux

Open Terminal and paste this command:

```bash
bash <(curl -sSL https://raw.githubusercontent.com/dvilelaf/micromech/main/scripts/quickstart.sh)
```

### Windows

Open a WSL terminal (search "WSL" in the Start menu) and paste the same command:

```bash
bash <(curl -sSL https://raw.githubusercontent.com/dvilelaf/micromech/main/scripts/quickstart.sh)
```

> If WSL is not installed, open PowerShell as Administrator and run `wsl --install`, then restart your computer. Docker Desktop must be set to use the WSL 2 backend (this is the default).

Then start micromech:

```bash
cd micromech
docker compose up -d
```

Open **http://localhost:8000** in your browser. You should see the micromech dashboard.

## First-Time Setup

The dashboard at **http://localhost:8000** walks you through a setup wizard:

**Step 1 — Create your wallet.** This is your on-chain identity. Pick a strong password. micromech stores your wallet locally — nobody else has access to it.

**Step 2 — Choose your chains.** Chains are different blockchain networks. Gnosis is the default and cheapest to start with. You can add more later.

**Step 3 — Fund your wallet.** Your wallet needs a small amount of cryptocurrency to pay for transaction fees (like postage stamps for the blockchain). The wizard shows your wallet address and tells you exactly how much you need.

**Step 4 — Deploy your mech.** Hit the deploy button. micromech registers your AI worker on the OLAS network. This takes a minute or two.

**Step 5 — Start earning.** Once deployed, micromech automatically listens for AI requests, solves them, and collects rewards. There is nothing else to do.

## Is It Working?

Visit **http://localhost:8000** at any time. You're in good shape if you see:

- **Status: Running** — your mech is online and listening for requests
- **Deliveries** increasing over time — your mech is solving tasks
- **Rewards** accumulating — you're earning OLAS tokens

It's normal for deliveries to come in waves. Some hours are busier than others.

## Remote Control via Telegram (Optional)

Control your mech from your phone:

1. Open Telegram, search for **@BotFather**, send `/newbot` and follow the prompts. Copy the **token** it gives you.
2. Search for **@userinfobot**, send it any message. Copy your **chat ID** (a number).
3. Open `micromech/secrets.env` in any text editor and fill in:
   ```
   telegram_token=your-bot-token-here
   telegram_chat_id=your-chat-id-here
   ```
4. Restart: `cd micromech && docker compose restart`

Now send `/status` to your bot to check on your mech.

## Day-to-Day Commands

Run these from inside the `micromech/` folder:

| Command | What it does |
|---|---|
| `docker compose up -d` | Start micromech |
| `docker compose down` | Stop micromech |
| `docker compose logs -f micromech` | View live logs (Ctrl+C to exit) |
| `docker compose pull && docker compose up -d` | Update to latest version |

micromech restarts automatically if your computer reboots.

## FAQ

**"Docker is not running"**
Open the Docker Desktop app. Wait until the whale icon stops animating.

**"Port 8000 already in use"**
Edit `docker-compose.yml` and change `127.0.0.1:8000:8000` to `127.0.0.1:8001:8000`, then open `http://localhost:8001`.

**"I closed the terminal and it stopped"**
Make sure you started with `docker compose up -d` (the `-d` flag makes it run in the background).

**"How much can I earn?"**
Depends on network demand. Check [staking.olas.network](https://staking.olas.network/) for current rates.

**"Is my wallet safe?"**
Your private key is encrypted with your password and stored only on your machine in `micromech/data/`. Back up this folder.

**"How do I uninstall?"**
Run `docker compose down` then delete the `micromech/` folder.

## Glossary

| Term | What it means |
|---|---|
| **Mech** | An AI worker on the blockchain that answers requests for a fee |
| **OLAS** | The token (cryptocurrency) you earn as rewards |
| **Staking** | Locking tokens to prove you're a reliable worker — micromech handles this |
| **Checkpoint** | Periodic on-chain proof your mech is active — called automatically |
| **Delivery** | A completed AI task your mech solved |
| **Tool** | An AI capability (text generation, prediction, etc.) |
| **Chain** | A blockchain network (Gnosis, Ethereum, etc.) |
| **Gas** | A small fee for blockchain transactions, like postage |

## For Developers

```bash
git clone https://github.com/dvilelaf/micromech
cd micromech && uv sync --all-extras
just check       # lint + types
just security    # gitleaks + bandit
just test        # pytest
```

## License

Apache-2.0
