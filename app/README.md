# PolySnipe App Runtime

This folder is the primary bot runtime.

## Files

- Entry point: `main.py`
- Env file: `.env` (private, never commit)
- Env template: `.env.example` (safe for repo)
- Config: `config.yaml`
- Dashboard: `http://127.0.0.1:8898`

## First-Time Setup

From repo root, use the public bootstrap:
- Windows: `setup-public.bat`
- macOS/Linux: `./setup-public.sh` (or `python3 scripts/setup_public.py`)
The script auto-creates `.env`, generates wallet + API credentials, and prints funding steps.

Then run:
- root: `start-hft.bat`
- or inside `app`: `.venv` python + `main.py`

If the dashboard shows **Wallet Needs Funding**, you can click:
- `Run Setup (Auto)` to generate credentials from UI (bootstrap only)
- `Copy Wallet` to fund the generated Polygon address
- Click `Restart Now` when prompted so new credentials load

Important:
- First-time users should run full setup from repo root first (`setup-public.bat` / `setup-public.sh`).
- Dashboard `Run Setup (Auto)` does not install dependencies; it assumes runtime is already running.

## Funding Reminder

Use your `POLYMARKET_WALLET_ADDRESS` from `.env` and fund on Polygon with:
- USDC.e
- small POL gas balance

Recommended bridge/swap:
- `https://sideshift.ai/a/trade`
- If wallet is empty, dashboard shows a funding card with SideShift link + copy-wallet button.

If network/routing blocks requests, use a VPN exit in Toronto or Netherlands.

## License

- Community use: non-commercial + share-alike (`../LICENSE`)
- Commercial use: separate paid commercial license (`../COMMERCIAL_LICENSE.md`)
