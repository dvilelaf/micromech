# micromech

**A lightweight mech for the [OLAS](https://olas.network/) network.** micromech runs an autonomous AI agent (called a "mech") that listens for on-chain requests, solves them using pluggable tools, and delivers results back on-chain.

No prior experience with OLAS or blockchain development is needed to get started.

<p align="center">
  <img width="30%" src="images/micromech.png">
</p>

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

Open Terminal and run from the directory where you want micromech installed (your home directory is a good default):

```bash
cd ~ && bash <(curl -sSL https://raw.githubusercontent.com/dvilelaf/micromech/main/scripts/quickstart.sh)
```

This creates a `micromech/` folder in the current directory. If you want to install in a system directory like `/opt`, run with `sudo` instead:

```bash
cd /opt && sudo bash <(curl -sSL https://raw.githubusercontent.com/dvilelaf/micromech/main/scripts/quickstart.sh)
```

### Windows

Open a WSL terminal (search "WSL" in the Start menu) and run:

```bash
cd ~ && bash <(curl -sSL https://raw.githubusercontent.com/dvilelaf/micromech/main/scripts/quickstart.sh)
```

> If WSL is not installed, open PowerShell as Administrator and run `wsl --install`, then restart your computer. Docker Desktop must be set to use the WSL 2 backend (this is the default).

Open **http://localhost:8090** in your browser. The setup wizard will guide you through creating your wallet, choosing a chain, and deploying your mech.

## Is It Working?

Visit **http://localhost:8090** at any time. You're in good shape if you see:

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

Run these from inside the `micromech/` folder. If you have [`just`](https://github.com/casey/just) installed, the shortcuts make things easier — otherwise use the `docker compose` equivalents.

| What you want to do | `just` shortcut | docker compose equivalent |
|---|---|---|
| Start micromech | `just up` | `docker compose up -d` |
| Stop micromech | `just down` | `docker compose down` |
| View live logs | `just logs` | `docker compose logs -f` |
| Update to latest image | `just update` | `docker compose pull && docker compose up -d` |
| Update `Justfile` + `docker-compose.yml` | `just update-config` | *(no equivalent)* |
| Check container status | `just status` | `docker compose ps` |
| Re-run setup wizard | `just init` | *(no equivalent)* |
| Run health check | `just doctor` | *(no equivalent)* |

micromech restarts automatically if your computer reboots.

## Your Files

After the quickstart, your `micromech/` folder contains:

| File | What it is | Notes |
|---|---|---|
| `secrets.env` | Passwords, API keys, Telegram token, custom RPC endpoints | **Never share.** This is the main file you'll edit. |
| `docker-compose.yml` | Docker configuration | Leave it alone unless you need to change ports. |
| `data/` | All persistent data (wallet, database, config, AI models) | **Back this up regularly.** |
| `data/config.yaml` | Mech settings: chains, mech addresses, staking contracts | You can edit it, but the web dashboard is easier. |
| `data/wallet.json` | Your encrypted wallet (private key + recovery phrase) | **Never share. Back this up.** |
| `data/micromech.db` | SQLite database of all requests — pending, delivered, failed | Grows over time. Safe to delete if disk space is tight (history only, no funds). |
| `data/backup/` | Automatic wallet backups created before any key change | Keep these. Delete old ones manually if needed. |
| `data/.hf_cache/` | Cached AI model files (downloaded on first use) | Large (~300 MB). Safe to delete — re-downloads automatically. |
| `updater.sh` | Auto-update script (called by the container on startup) | Don't touch. |
| `Justfile` | Command shortcuts (requires `just`) | Optional convenience, safe to ignore. |

> **Backup reminder:** Copy the entire `data/` folder somewhere safe. If you lose `data/wallet.json`, you lose access to your wallet.

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
