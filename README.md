# PolySnipe

PolySnipe is an HFT-first Polymarket runtime with:
- main bot runtime (`app/main.py`)
- live dashboard (`http://127.0.0.1:8898`)
- monitor + optimizer views

## Fastest Setup (Recommended)

### Windows
1. Clone the repo.
2. Run:
   - `setup-public.bat`
3. Start:
   - `start-hft.bat`
4. Open:
   - `http://127.0.0.1:8898`

### macOS / Linux
1. Clone the repo.
2. Run:
   - `./setup-public.sh`
   - (or: `python3 scripts/setup_public.py`)
3. Start:
   - `cd app`
   - `./.venv/bin/python main.py`
4. Open:
   - `http://127.0.0.1:8898`

## Dashboard Setup Path

If dashboard is already running and wallet is not initialized/funded:
1. Open `http://127.0.0.1:8898`
2. Use the **Wallet Needs Funding** card
3. Click **Run Setup (Auto)** to generate wallet + Polymarket API credentials
4. Click **Copy Wallet** and fund that address on Polygon
5. When setup finishes, click **Restart Now** in the restart prompt so credentials are loaded

Important:
- Dashboard **Run Setup (Auto)** is credential/bootstrap only.
- First-time install on a clean machine should always run full setup first:
  - Windows: `setup-public.bat`
  - macOS/Linux: `python3 scripts/setup_public.py`

## Manual Setup (Advanced)

1. Create venv:
   - Windows: `python -m venv app\\.venv`
   - macOS/Linux: `python3 -m venv app/.venv`
2. Install requirements:
   - Windows: `app\\.venv\\Scripts\\python.exe -m pip install -r app\\requirements.txt`
   - macOS/Linux: `app/.venv/bin/python -m pip install -r app/requirements.txt`
3. Generate `.env` + credentials:
   - `python scripts/setup_public.py --no-venv --no-pip`
   - If runtime is already live on `127.0.0.1:8898`, setup auto-requests a restart.
4. Start bot:
   - Windows: `start-hft.bat`
   - macOS/Linux: `cd app && ./.venv/bin/python main.py`

## Funding Requirements

Fund your `POLYMARKET_WALLET_ADDRESS` (in `app/.env`) on **Polygon (PoS)** with:
- **USDC.e** (trading balance)
- **POL** (gas)

Recommended (KYC-free): `https://sideshift.ai/a/trade`

Connectivity tip: if routing is blocked, use a VPN exit in Toronto or Netherlands.

## What Setup Script Handles

- creates `app/.venv`
- installs `app/requirements.txt`
- creates `app/.env` from `app/.env.example` (if missing)
- auto-generates wallet + Polymarket API credentials in `app/.env`
- prints exact next steps

Dashboard card setup (`Run Setup (Auto)`) does not install dependencies; it assumes runtime is already up.

## Security

- Never commit real secrets from `app/.env`
- `app/.env.example` is the safe template for git
- Do not commit DB/log files

## Licensing

This project is released under the **PolySnipe Community Non-Commercial Share-Alike License**:
- Non-commercial use only
- If you distribute or publicly run a modified version, you must share source for your modifications under the same license
- Commercial use requires a separate paid commercial license

See:
- `LICENSE`
- `COMMERCIAL_LICENSE.md`
