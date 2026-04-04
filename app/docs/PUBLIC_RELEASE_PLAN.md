# Public Release Plan (PolySnipe)

This document captures the public-release hardening plan and what was implemented.

## Goals

1. Do not publish secrets (`app/.env`, `.bak/.env`, API keys, private wallet defaults).
2. Make setup one-command for non-technical users.
3. Provide clear funding instructions (Polygon USDC.e + POL gas).
4. Keep local private runtime files intact for ongoing private usage.
5. Keep `.bak/` legacy runtime in repo, but remove dead temp artifacts inside it.

## Implemented

### 1) Secret and runtime safety

- `.gitignore` updated to:
  - ignore nested `.env` files
  - allow `.env.example` files
  - ignore `app/.venv/`, `app/data/`, `app/logs/`
- Removed hardcoded wallet/API defaults from:
  - `app/dashboard/app.py`
  - `app/snipe_view/app.py`
  - `.bak/dashboard/app.py`
  - `.bak/snipe_view/app.py`
  - `.bak/check_balance.py`
  - `.bak/check_allowance.py`
- Wallet readouts now use:
  - `POLYMARKET_WALLET_ADDRESS`
  - `POLYGONSCAN_API_KEY`
- If missing, balance fetch safely returns `None`.

### 2) Dead backup cleanup

- Kept `.bak/` in repo for legacy tinkering.
- Removed obvious dead temp/debug artifacts from `.bak`:
  - `gitlog.txt`, `settings.txt`
  - `temp_*`, `tmp_*` dump files

### 3) Public env template

- Added `app/.env.example` with:
  - required Polymarket keys
  - optional `POLYGONSCAN_API_KEY`
  - optional monitor tuning key
- Added `.bak/.env.example` for legacy runtime users.

### 4) One-command noob setup

- Added `scripts/setup_public.py`:
  - creates `app/.venv`
  - installs `app/requirements.txt`
  - creates `app/.env` from template if missing
  - optionally prompts for credentials
  - prints funding + launch instructions
- Added launchers:
  - `setup-public.bat` (Windows)
  - `setup-public.sh` (macOS/Linux)

### 5) User docs

- Updated `README.md` and `app/README.md` with:
  - quick start
  - funding requirements
  - security notes
  - SideShift referral flow: `https://sideshift.ai/a/trade`

## Manual publish checks before creating public repo

1. `git status` and verify no private `.env` tracked.
2. `git ls-files | rg "\.env$"` should return no real env files.
3. Confirm DB/log artifacts are not tracked.
4. Confirm `setup-public.bat` works on a clean machine.
5. Confirm dashboard boots after setup (`http://127.0.0.1:8898`).
