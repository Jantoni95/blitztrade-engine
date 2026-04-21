#!/usr/bin/env python3
"""
Build one Windows installer executable for BlitzTrade using Inno Setup.

Output: dist/BlitzTradeInstaller.exe

The installer contains BlitzTrade.exe and optionally downloads/launches the
official IB Gateway installer during setup with explicit user consent.

Usage:
    python build_installer.py
    python build_installer.py --version 1.1.4
    python build_installer.py --iscc "C:\\Program Files (x86)\\Inno Setup 6\\ISCC.exe"
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
DIST_DIR = SCRIPT_DIR / "dist"
APP_EXE = DIST_DIR / "BlitzTrade.exe"
ISS_PATH = SCRIPT_DIR / "installer" / "BlitzTradeInstaller.iss"


def _discover_version() -> str:
    launcher_path = SCRIPT_DIR / "launcher.py"
    if not launcher_path.exists():
        return "0.0.0"

    text = launcher_path.read_text(encoding="utf-8", errors="replace")
    m = re.search(r'^APP_VERSION\s*=\s*"([^"]+)"', text, flags=re.MULTILINE)
    if not m:
        return "0.0.0"
    return m.group(1)


def _resolve_iscc(explicit_path: str | None) -> str:
    if explicit_path:
        return explicit_path

    candidates = [
        r"C:\Program Files (x86)\Inno Setup 6\ISCC.exe",
        r"C:\Program Files\Inno Setup 6\ISCC.exe",
    ]
    for c in candidates:
        if Path(c).exists():
            return c

    found = shutil.which("iscc")
    if found:
        return found

    raise FileNotFoundError(
        "ISCC.exe was not found. Install Inno Setup 6 or pass --iscc <path>."
    )


def build_installer(version: str, iscc_path: str) -> int:
    if not APP_EXE.exists():
        print(
            f"[!] Missing {APP_EXE}. Build BlitzTrade.exe first (python build_app.py --version {version})."
        )
        return 2

    if not ISS_PATH.exists():
        print(f"[!] Missing installer script: {ISS_PATH}")
        return 3

    DIST_DIR.mkdir(parents=True, exist_ok=True)

    cmd = [
        iscc_path,
        f"/DMyAppVersion={version}",
        str(ISS_PATH),
    ]

    print("  Running:")
    print("  " + " ".join(cmd))
    res = subprocess.run(cmd, cwd=str(SCRIPT_DIR))
    if res.returncode != 0:
        print(f"[!] Inno Setup compilation failed with exit code {res.returncode}")
        return res.returncode

    out_file = DIST_DIR / "BlitzTradeInstaller.exe"
    if out_file.exists():
        print(f"[OK] Created installer: {out_file}")
    else:
        print("[!] ISCC finished but expected output was not found.")
        return 4

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Build BlitzTradeInstaller.exe")
    parser.add_argument("--version", help="Installer app version to stamp")
    parser.add_argument("--iscc", help="Path to ISCC.exe (Inno Setup compiler)")
    args = parser.parse_args()

    version = args.version or _discover_version()
    iscc_path = _resolve_iscc(args.iscc)

    print(f"  Version: {version}")
    print(f"  ISCC: {iscc_path}")

    return build_installer(version, iscc_path)


if __name__ == "__main__":
    raise SystemExit(main())
