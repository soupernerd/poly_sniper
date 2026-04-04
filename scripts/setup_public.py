#!/usr/bin/env python3
"""Public setup bootstrap for PolySnipe."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import textwrap
import urllib.request
import venv
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
APP_DIR = REPO_ROOT / "app"
VENV_DIR = APP_DIR / ".venv"
REQ_FILE = APP_DIR / "requirements.txt"
ENV_EXAMPLE = APP_DIR / ".env.example"
ENV_FILE = APP_DIR / ".env"

CLOB_URL = "https://clob.polymarket.com"
CLOB_CHAIN_ID = 137
CLOB_SIGNATURE_TYPE = 0

REQUIRED_KEYS = [
    "POLYMARKET_PRIVATE_KEY",
    "POLYMARKET_WALLET_ADDRESS",
    "POLYMARKET_API_KEY",
    "POLYMARKET_API_SECRET",
    "POLYMARKET_API_PASSPHRASE",
]
OPTIONAL_KEYS = ["POLYGONSCAN_API_KEY", "MONITOR_V2_COMPACT_METADATA_WRITES"]


def _venv_python() -> Path:
    if os.name == "nt":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def _run(cmd: list[str], cwd: Path | None = None) -> None:
    print("+", " ".join(cmd))
    subprocess.check_call(cmd, cwd=str(cwd) if cwd else None)


def _read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _render_env(example_path: Path, values: dict[str, str]) -> str:
    lines: list[str] = []
    seen: set[str] = set()
    for line in example_path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in line:
            lines.append(line)
            continue
        key, _ = line.split("=", 1)
        key = key.strip()
        if key in values and values[key] != "":
            lines.append(f"{key}={values[key]}")
        else:
            lines.append(line)
        seen.add(key)

    extras = [k for k in values.keys() if k not in seen]
    if extras:
        lines.append("")
        lines.append("# Additional local keys")
        for key in extras:
            lines.append(f"{key}={values[key]}")

    return "\n".join(lines).rstrip() + "\n"


def _is_missing(value: str) -> bool:
    v = str(value or "").strip()
    if not v:
        return True
    low = v.lower()
    if low.startswith("<"):
        return True
    if "your_" in low or "your" in low and "here" in low:
        return True
    if "yourpolygonwalletaddress" in low:
        return True
    return False


def _generate_wallet_and_api(*, private_key: str = "", wallet_address: str = "") -> dict[str, str]:
    py = str(_venv_python())
    env = os.environ.copy()
    env["PS_PRIVATE_KEY"] = private_key or ""
    env["PS_WALLET"] = wallet_address or ""
    env["PS_CLOB_URL"] = CLOB_URL
    env["PS_CHAIN_ID"] = str(CLOB_CHAIN_ID)
    env["PS_SIGNATURE_TYPE"] = str(CLOB_SIGNATURE_TYPE)

    helper = textwrap.dedent(
        """
        import json
        import os
        from eth_account import Account
        from py_clob_client.client import ClobClient

        private_key = (os.getenv("PS_PRIVATE_KEY", "") or "").strip()
        wallet = (os.getenv("PS_WALLET", "") or "").strip()
        clob_url = (os.getenv("PS_CLOB_URL", "") or "").strip()
        chain_id = int(os.getenv("PS_CHAIN_ID", "137") or 137)
        signature_type = int(os.getenv("PS_SIGNATURE_TYPE", "0") or 0)

        if not private_key:
            acct = Account.create()
            private_key = acct.key.hex()
            wallet = acct.address
        else:
            acct = Account.from_key(private_key)
            if not wallet:
                wallet = acct.address

        client = ClobClient(
            clob_url,
            key=private_key,
            chain_id=chain_id,
            signature_type=signature_type,
            funder=wallet,
        )
        creds = client.create_or_derive_api_creds()
        print(json.dumps({
            "private_key": private_key,
            "wallet_address": wallet,
            "api_key": creds.api_key,
            "api_secret": creds.api_secret,
            "api_passphrase": creds.api_passphrase,
        }))
        """
    ).strip()

    proc = subprocess.run(
        [py, "-c", helper],
        cwd=str(APP_DIR),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        msg = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"Credential generation failed: {msg}")

    out = (proc.stdout or "").strip()
    if not out:
        raise RuntimeError("Credential generation returned empty output")
    try:
        return json.loads(out.splitlines()[-1])
    except Exception as exc:
        raise RuntimeError(f"Could not parse generated credentials: {exc}") from exc


def _ensure_venv() -> None:
    if VENV_DIR.exists() and _venv_python().exists():
        print(f"[ok] venv exists: {VENV_DIR}")
        return
    print(f"[setup] Creating virtual environment at {VENV_DIR}")
    venv.EnvBuilder(with_pip=True, clear=False).create(VENV_DIR)


def _install_requirements() -> None:
    py = str(_venv_python())
    _run([py, "-m", "pip", "install", "--upgrade", "pip"], cwd=APP_DIR)
    _run([py, "-m", "pip", "install", "-r", str(REQ_FILE)], cwd=APP_DIR)


def _ensure_env(*, rotate_credentials: bool = False, overwrite_env: bool = False) -> dict[str, str]:
    if not ENV_EXAMPLE.exists():
        raise FileNotFoundError(f"Missing template: {ENV_EXAMPLE}")

    if overwrite_env:
        shutil.copy2(ENV_EXAMPLE, ENV_FILE)
        print(f"[setup] Overwrote {ENV_FILE} from template")
    elif not ENV_FILE.exists():
        shutil.copy2(ENV_EXAMPLE, ENV_FILE)
        print(f"[setup] Created {ENV_FILE} from template")
    else:
        print(f"[ok] Existing env found: {ENV_FILE}")

    values = _read_env(ENV_FILE)

    need_private = _is_missing(values.get("POLYMARKET_PRIVATE_KEY", ""))
    need_wallet = _is_missing(values.get("POLYMARKET_WALLET_ADDRESS", ""))
    need_api = any(_is_missing(values.get(k, "")) for k in ("POLYMARKET_API_KEY", "POLYMARKET_API_SECRET", "POLYMARKET_API_PASSPHRASE"))
    if rotate_credentials:
        need_private = True
        need_wallet = True
        need_api = True

    if need_private or need_wallet or need_api:
        print("[setup] Auto-generating wallet + Polymarket API credentials")
        generated = _generate_wallet_and_api(private_key="", wallet_address="")
        values["POLYMARKET_PRIVATE_KEY"] = generated["private_key"]
        values["POLYMARKET_WALLET_ADDRESS"] = generated["wallet_address"]
        values["POLYMARKET_API_KEY"] = generated["api_key"]
        values["POLYMARKET_API_SECRET"] = generated["api_secret"]
        values["POLYMARKET_API_PASSPHRASE"] = generated["api_passphrase"]
        print("[ok] Credentials generated and written to app/.env")

    if _is_missing(values.get("MONITOR_V2_COMPACT_METADATA_WRITES", "")):
        values["MONITOR_V2_COMPACT_METADATA_WRITES"] = "1"

    for key in OPTIONAL_KEYS:
        values.setdefault(key, "")

    ENV_FILE.write_text(_render_env(ENV_EXAMPLE, values), encoding="utf-8")
    return _read_env(ENV_FILE)


def _print_next_steps(values: dict[str, str]) -> None:
    wallet = values.get("POLYMARKET_WALLET_ADDRESS", "").strip() or "<set POLYMARKET_WALLET_ADDRESS in app/.env>"

    msg = textwrap.dedent(
        f"""

        Setup complete.

        Next steps:
        1) Fund your Polygon wallet:
           - Wallet address: {wallet}
           - Required assets: USDC.e (Polygon/PoS) + a small amount of POL for gas.

        2) Recommended bridge/swap (KYC-free):
           - https://sideshift.ai/a/trade
           - Swap from other chains/assets into Polygon USDC.e.

        3) If exchange/network routing blocks requests, use a VPN exit in:
           - Toronto, or
           - Netherlands.

        4) Start PolySnipe:
           - Windows: start-hft.bat
           - Manual: app/.venv Python -> run app/main.py

        5) Open dashboard: http://127.0.0.1:8898

        Security reminders:
        - Never share or commit app/.env.
        - app/.env.example is the only template intended for git.
        """
    ).strip("\n")
    print(msg)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PolySnipe public setup bootstrap")
    parser.add_argument(
        "--no-venv",
        action="store_true",
        help="Skip virtualenv creation/check (assumes current interpreter is ready).",
    )
    parser.add_argument(
        "--no-pip",
        action="store_true",
        help="Skip pip install/upgrade step.",
    )
    parser.add_argument(
        "--no-restart",
        action="store_true",
        help="Do not request runtime restart after setup.",
    )
    parser.add_argument(
        "--rotate-credentials",
        action="store_true",
        help="Always generate a new wallet + Polymarket API credentials.",
    )
    parser.add_argument(
        "--overwrite-env",
        action="store_true",
        help="Overwrite app/.env from app/.env.example before writing credentials.",
    )
    return parser.parse_args(argv)


def _request_runtime_restart() -> bool:
    url = "http://127.0.0.1:8898/api/control/restart"
    req = urllib.request.Request(
        url,
        data=b"{}",
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=4) as resp:
            if int(getattr(resp, "status", 200)) >= 300:
                return False
            _ = resp.read()
        return True
    except Exception:
        return False


def main(argv: list[str] | None = None) -> int:
    if sys.version_info < (3, 11):
        print("Python 3.11+ is required.")
        return 1

    args = _parse_args(argv)
    print("PolySnipe public bootstrap")
    print(f"Repo: {REPO_ROOT}")
    print(f"App : {APP_DIR}")

    try:
        if not args.no_venv:
            _ensure_venv()
        elif not _venv_python().exists():
            print("[error] --no-venv was passed but app/.venv is missing.")
            return 1
        if not args.no_pip:
            _install_requirements()
        env_values = _ensure_env(
            rotate_credentials=args.rotate_credentials,
            overwrite_env=args.overwrite_env,
        )
        _print_next_steps(env_values)
        if not args.no_restart:
            if _request_runtime_restart():
                print("[ok] Running runtime detected. Restart requested to load new credentials.")
            else:
                print("[info] Runtime restart not requested (runtime not running at 127.0.0.1:8898).")
        return 0
    except subprocess.CalledProcessError as exc:
        print(f"[error] Command failed with exit code {exc.returncode}")
        return exc.returncode or 1
    except Exception as exc:
        print(f"[error] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
