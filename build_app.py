#!/usr/bin/env python3
"""
Build a distributable BlitzTrade binary.

  macOS   → dist/BlitzTrade (standalone binary)
  Windows → dist/BlitzTrade.exe

Uses Nuitka to compile Python to C — produces native machine code.
No .pyc files, no bytecode to extract. Best anti-reverse-engineering.

Usage:
    python build_app.py              # build for current platform
    python build_app.py --version 1.2.0   # override version stamp

Requires:  pip install nuitka ordered-set pywebview
"""

import argparse
import importlib
import os
import platform
import shutil
import stat
import struct
import subprocess
from pathlib import Path
import sys
import math
import zlib

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DIST_DIR = os.path.join(SCRIPT_DIR, "dist")
BUILD_DIR = os.path.join(SCRIPT_DIR, "build")

# Files to bundle alongside the Python code
DATA_FILES = [
    "index.html",
    "analytics.html",
    "help.html",
    "serve.py",
    "trades.json",
    "blitz_trade.ico",
]
DATA_FILES.extend(
    sorted(
        str(path.relative_to(SCRIPT_DIR)).replace("\\", "/")
        for path in Path(SCRIPT_DIR, "release_notes").glob("*.json")
        if path.is_file()
    )
)


def _package_dir(module_name):
    module = importlib.import_module(module_name)
    if not getattr(module, "__file__", None):
        raise RuntimeError(f"Cannot resolve package directory for {module_name}")
    return os.path.dirname(os.path.abspath(module.__file__))


# ── Icon rendering (reused from build_launcher.py) ──────────


def _dist_to_segment(px, py, x0, y0, x1, y1):
    dx, dy = x1 - x0, y1 - y0
    len2 = dx * dx + dy * dy
    if len2 == 0:
        return math.hypot(px - x0, py - y0)
    t = max(0.0, min(1.0, ((px - x0) * dx + (py - y0) * dy) / len2))
    return math.hypot(px - (x0 + t * dx), py - (y0 + t * dy))


def _bolt_distance(px, py, bolt):
    d = float("inf")
    n = len(bolt)
    for i in range(n):
        x0, y0 = bolt[i]
        x1, y1 = bolt[(i + 1) % n]
        d = min(d, _dist_to_segment(px, py, x0, y0, x1, y1))
    return d


def _sample(x, y, bolt, stroke_w, cx, cy, corner):
    dx = max(0.0, abs(x - cx) - (cx - corner))
    dy = max(0.0, abs(y - cy) - (cy - corner))
    if dx * dx + dy * dy > corner * corner:
        return (0, 0, 0, 0)
    if _bolt_distance(x, y, bolt) <= stroke_w:
        return (0x58, 0xA6, 0xFF, 0xFF)
    return (0x0D, 0x11, 0x17, 0xFF)


def draw_icon(size):
    pixels = bytearray(size * size * 4)
    bolt_svg = [(13, 2), (3, 14), (12, 14), (11, 22), (21, 10), (12, 10)]
    pad = size * 0.15
    inner = size - 2 * pad
    bolt = [(pad + bx / 24.0 * inner, pad + by / 24.0 * inner) for bx, by in bolt_svg]
    stroke_w = size * (2.0 / 24.0) * 0.55
    cx = cy = size / 2.0
    corner = size * 0.18
    SS = 4
    step = 1.0 / SS
    start = step / 2.0
    for py in range(size):
        for px in range(size):
            rt = gt = bt = at = 0
            for sy in range(SS):
                fy = py + start + sy * step
                for sx in range(SS):
                    fx = px + start + sx * step
                    r, g, b, a = _sample(fx, fy, bolt, stroke_w, cx, cy, corner)
                    rt += r
                    gt += g
                    bt += b
                    at += a
            n = SS * SS
            off = (py * size + px) * 4
            pixels[off] = rt // n
            pixels[off + 1] = gt // n
            pixels[off + 2] = bt // n
            pixels[off + 3] = at // n
    return bytes(pixels)


def rgba_to_png(rgba, w, h):
    raw = b""
    stride = w * 4
    for y in range(h):
        raw += b"\x00" + rgba[y * stride : (y + 1) * stride]

    def chunk(ctype, data):
        c = ctype + data
        return (
            struct.pack(">I", len(data))
            + c
            + struct.pack(">I", zlib.crc32(c) & 0xFFFFFFFF)
        )

    ihdr = struct.pack(">IIBBBBB", w, h, 8, 6, 0, 0, 0)
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", ihdr)
        + chunk(b"IDAT", zlib.compress(raw, 9))
        + chunk(b"IEND", b"")
    )


def make_ico(path):
    entries = []
    for s in [256, 48, 32, 16]:
        entries.append((s, rgba_to_png(draw_icon(s), s, s)))
    hdr = struct.pack("<HHH", 0, 1, len(entries))
    offset = 6 + 16 * len(entries)
    dir_entries = img_data = b""
    for s, png in entries:
        w = 0 if s == 256 else s
        dir_entries += struct.pack("<BBBBHHII", w, w, 0, 0, 1, 32, len(png), offset)
        offset += len(png)
        img_data += png
    with open(path, "wb") as f:
        f.write(hdr + dir_entries + img_data)


def make_icns(path):
    body = b""
    for tag, s in [(b"ic08", 256), (b"ic07", 128)]:
        png = rgba_to_png(draw_icon(s), s, s)
        body += tag + struct.pack(">I", len(png) + 8) + png
    with open(path, "wb") as f:
        f.write(b"icns" + struct.pack(">I", len(body) + 8) + body)


# ── Version stamping ────────────────────────────────────────


def stamp_version(version, target_path):
    """Write version + IS_PACKAGED=True into a copy of launcher.py at target_path."""
    launcher = os.path.join(SCRIPT_DIR, "launcher.py")
    with open(launcher, "r") as f:
        content = f.read()
    import re

    content = re.sub(
        r'APP_VERSION\s*=\s*"[^"]*"',
        f'APP_VERSION = "{version}"',
        content,
    )
    content = re.sub(
        r"^IS_PACKAGED\s*=\s*.*$",
        "IS_PACKAGED = True",
        content,
        flags=re.MULTILINE,
    )
    with open(target_path, "w") as f:
        f.write(content)
    print(
        f"  Stamped version {version} -> {os.path.basename(target_path)} (source untouched)"
    )


def stamp_cognito(target_path):
    """Fetch Cognito IDs from CloudFormation and write into the build copy at target_path."""
    import re as _re

    stack = "blitztrade-web"
    region = "eu-central-1"
    print(f"  Fetching Cognito config from {stack} stack...")
    try:
        raw = subprocess.check_output(
            [
                "aws",
                "cloudformation",
                "describe-stacks",
                "--stack-name",
                stack,
                "--region",
                region,
                "--query",
                "Stacks[0].Outputs",
                "--output",
                "json",
            ],
            text=True,
        )
        import json as _json

        outputs = {o["OutputKey"]: o["OutputValue"] for o in _json.loads(raw)}
    except Exception as e:
        print(f"  [!] Could not fetch stack outputs: {e}")
        pool_id = os.environ.get("COGNITO_USER_POOL_ID", "")
        client_id = os.environ.get("COGNITO_CLIENT_ID", "")
        if pool_id and client_id:
            print(f"  Using COGNITO_USER_POOL_ID/COGNITO_CLIENT_ID env vars.")
        else:
            print(f"  Set COGNITO_USER_POOL_ID and COGNITO_CLIENT_ID env vars for dev.")
            return
    else:
        pool_id = outputs.get("UserPoolId", "")
        client_id = outputs.get("DesktopClientId", "")
    if not pool_id or not client_id:
        print(
            f"  [!] Missing outputs: UserPoolId={pool_id}, DesktopClientId={client_id}"
        )
        return
    with open(target_path, "r") as f:
        content = f.read()
    content = _re.sub(
        r'COGNITO_USER_POOL_ID\s*=\s*"[^"]*"',
        f'COGNITO_USER_POOL_ID = "{pool_id}"',
        content,
    )
    content = _re.sub(
        r'COGNITO_CLIENT_ID\s*=\s*"[^"]*"',
        f'COGNITO_CLIENT_ID = "{client_id}"',
        content,
    )
    with open(target_path, "w") as f:
        f.write(content)
    print(f"  Stamped Cognito: pool={pool_id}, client={client_id}")


def stamp_download_url(target_path):
    """Fetch the download API URL from the infra stack and write into the build copy."""
    import re as _re

    stack = "blitztrade-infra"
    region = "us-east-1"
    print(f"  Fetching download URL from {stack} stack...")
    try:
        raw = subprocess.check_output(
            [
                "aws", "cloudformation", "describe-stacks",
                "--stack-name", stack,
                "--region", region,
                "--query", "Stacks[0].Outputs[?OutputKey=='DownloadUrl'].OutputValue",
                "--output", "text",
            ],
            text=True,
        ).strip()
    except Exception as e:
        print(f"  [!] Could not fetch stack outputs: {e}")
        raw = os.environ.get("BLITZ_UPDATE_URL", "")
        if raw:
            print(f"  Using BLITZ_UPDATE_URL env var.")
        else:
            print(f"  Set BLITZ_UPDATE_URL env var for dev.")
            return
    if not raw:
        print(f"  [!] DownloadUrl output is empty.")
        return
    with open(target_path, "r") as f:
        content = f.read()
    content = _re.sub(
        r'(BLITZ_UPDATE_URL",\s*\n\s*)"[^"]*"',
        f'\\1"{raw}"',
        content,
    )
    with open(target_path, "w") as f:
        f.write(content)
    print(f"  Stamped download URL: {raw}")


# ── Build ────────────────────────────────────────────────────


def build(version, onefile_windows=True, windows_console=False):
    print(f"\n>> Building BlitzTrade v{version} for {platform.system()}\n")

    # Stamp into a temporary copy — never mutates the source launcher.py
    import tempfile

    _launcher_tmp_fd, _launcher_tmp = tempfile.mkstemp(
        suffix="_launcher.py", dir=SCRIPT_DIR
    )
    os.close(_launcher_tmp_fd)
    try:
        stamp_version(version, _launcher_tmp)
        stamp_cognito(_launcher_tmp)
        stamp_download_url(_launcher_tmp)
        # Fail fast if stamping produced invalid Python.
        with open(_launcher_tmp, "r", encoding="utf-8") as _f:
            compile(_f.read(), _launcher_tmp, "exec")
    except Exception:
        os.remove(_launcher_tmp)
        raise

    webview_dir = None
    pythonnet_dir = None
    if platform.system() == "Windows":
        webview_dir = _package_dir("webview")
        pythonnet_dir = _package_dir("pythonnet")

    onefile_tempdir = None
    if platform.system() == "Windows":
        # Keep a stable per-user extraction location without hardcoding a
        # machine-specific absolute path from the build host.
        onefile_tempdir = "{CACHE_DIR}/BlitzTrade/onefile_runtime"

    os.makedirs(DIST_DIR, exist_ok=True)

    # Prepare icon (also used at runtime by pywebview for taskbar)
    ico_path = os.path.join(SCRIPT_DIR, "blitz_trade.ico")
    make_ico(ico_path)
    if platform.system() == "Windows":
        icon_arg = f"--windows-icon-from-ico={ico_path}"
    elif platform.system() == "Darwin":
        icns_path = os.path.join(SCRIPT_DIR, "blitz_trade.icns")
        make_icns(icns_path)
        icon_arg = f"--macos-app-icon={icns_path}"
    else:
        icon_arg = None

    # Nuitka command — compiles Python to C, no .pyc files
    cmd = [
        sys.executable,
        "-m",
        "nuitka",
        "--output-dir=" + DIST_DIR,
        "--assume-yes-for-downloads",
        # Include data files
    ]

    # Keep packaging stable even if the active environment contains extra dev-only
    # finance/scraping libraries that the app does not use at runtime.
    for module_name in [
        "yfinance",
        "yahooquery",
        "pandas",
        "bs4",
        "lxml",
        "html5lib",
        "setuptools",
    ]:
        cmd.append(f"--nofollow-import-to={module_name}")

    # macOS pywebview needs Foundation framework → use --mode=app
    if platform.system() == "Darwin":
        cmd.append("--mode=app")
    else:
        cmd.append("--standalone")
        if platform.system() == "Windows" and onefile_windows:
            cmd.append("--onefile")
            # Use Nuitka runtime variables so the path resolves correctly on each
            # end-user machine (including CI-built binaries).
            cmd.append(f"--onefile-tempdir-spec={onefile_tempdir}")
        # Let Nuitka's pywebview plugin manage backend module decisions.
        cmd.append("--enable-plugin=pywebview")
        cmd.append(
            f"--windows-console-mode={'force' if windows_console else 'disable'}"
        )
        cmd.append("--output-filename=BlitzTrade.exe")

    for fname in DATA_FILES:
        src = os.path.join(SCRIPT_DIR, fname)
        if os.path.exists(src):
            cmd.append(f"--include-data-files={src}=./{fname}")

    # Include packages that Nuitka might not auto-detect
    # Note: do not force-include full webview package, it conflicts with
    # Nuitka's built-in pywebview handling on recent versions.
    for pkg in [
        "ib_insync",
        "aiohttp",
        "multidict",
        "yarl",
        "aiosignal",
        "frozenlist",
        "attr",
    ]:
        cmd.append(f"--include-package={pkg}")

    if platform.system() == "Windows":
        cmd.extend(
            [
                "--include-module=clr",
                "--include-module=_cffi_backend",
                "--include-module=webview.platforms.winforms",
                "--include-module=webview.platforms.edgechromium",
                "--include-package=pythonnet",
                "--include-package=clr_loader",
                "--include-package=cffi",
                "--include-package=pycparser",
                "--include-package-data=pythonnet",
                "--include-package-data=clr_loader",
                "--include-package-data=webview",
                f"--include-data-dir={os.path.join(webview_dir, 'lib')}=webview/lib",
                f"--include-data-dir={os.path.join(webview_dir, 'js')}=webview/js",
                f"--include-data-dir={os.path.join(pythonnet_dir, 'runtime')}=pythonnet/runtime",
            ]
        )

    # Include serve.py as a module
    cmd.append(f"--include-module=serve")

    if icon_arg:
        cmd.append(icon_arg)

    # Platform-specific
    if platform.system() == "Darwin":
        cmd.append("--macos-app-name=BlitzTrade")
    elif platform.system() == "Windows":
        cmd.append("--windows-company-name=BlitzTrade")
        cmd.append("--windows-product-name=BlitzTrade")
        cmd.append(f"--windows-file-version={version}")
        cmd.append(f"--windows-product-version={version}")

    # Entry point — use the stamped temp copy
    cmd.append(_launcher_tmp)

    print("  Running Nuitka (compiling to C)...")
    try:
        subprocess.check_call(cmd)
    finally:
        # Always remove the temp stamped copy so source stays clean
        try:
            os.remove(_launcher_tmp)
        except Exception:
            pass

    # Nuitka output paths differ per platform/mode
    if platform.system() == "Windows":
        if onefile_windows:
            out = os.path.join(DIST_DIR, "BlitzTrade.exe")
        else:
            out = os.path.join(DIST_DIR, "BlitzTrade.dist", "BlitzTrade.exe")
    elif platform.system() == "Darwin":
        # --mode=app produces launcher.app — rename it
        nuitka_app = os.path.join(DIST_DIR, "launcher.app")
        out = os.path.join(DIST_DIR, "BlitzTrade.app")
        if os.path.exists(nuitka_app):
            if os.path.exists(out):
                shutil.rmtree(out)
            os.rename(nuitka_app, out)
        # Also find the actual binary inside for size reporting
        inner_bin = os.path.join(out, "Contents", "MacOS", "launcher")
        if os.path.exists(inner_bin):
            out_for_size = inner_bin
        else:
            out_for_size = out
    else:
        out = os.path.join(DIST_DIR, "launcher")

    if os.path.exists(out):
        target = out_for_size if platform.system() == "Darwin" else out
        size_mb = os.path.getsize(target) / (1024 * 1024)
        print(f"\n  [OK] {out}  ({size_mb:.1f} MB)")
    else:
        # List what Nuitka actually produced for debugging
        print(f"\n  [FAIL] Expected {out} not found. Contents of {DIST_DIR}:")
        if os.path.isdir(DIST_DIR):
            for item in os.listdir(DIST_DIR):
                print(f"    {item}")
        sys.exit(1)

    # Cleanup build artifacts
    for f in ["blitz_trade.ico", "blitz_trade.icns"]:
        p = os.path.join(SCRIPT_DIR, f)
        if os.path.exists(p):
            os.remove(p)
    for d in [
        BUILD_DIR,
        os.path.join(DIST_DIR, "launcher.build"),
        os.path.join(DIST_DIR, "launcher.dist"),
        os.path.join(DIST_DIR, "launcher.onefile-build"),
    ]:
        if os.path.isdir(d):
            shutil.rmtree(d)

    print("\nDone!")

    # Publish source to public blitztrade-engine repo
    publish_script = os.path.join(SCRIPT_DIR, "publish_source.py")
    if os.path.exists(publish_script):
        print(f"\n>> Publishing source to blitztrade-engine (v{version})...")
        try:
            subprocess.run(
                [sys.executable, publish_script, "--tag", f"v{version}", "--msg", f"Release v{version}"],
                cwd=SCRIPT_DIR,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"  [WARN] Source publish failed: {e}")
        except Exception as e:
            print(f"  [WARN] Source publish skipped: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build BlitzTrade desktop app")
    parser.add_argument(
        "--version", default="1.0.0", help="Version string (e.g. 1.2.0)"
    )
    parser.add_argument(
        "--standalone",
        action="store_true",
        help=(
            "Windows only: build a standalone folder instead of onefile. "
            "Default on Windows is onefile."
        ),
    )
    parser.add_argument(
        "--windows-console",
        action="store_true",
        help="Windows only: keep the console visible for debug builds.",
    )
    args = parser.parse_args()
    build(
        args.version,
        onefile_windows=not args.standalone,
        windows_console=args.windows_console,
    )
