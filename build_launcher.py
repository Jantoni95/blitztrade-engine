#!/usr/bin/env python3
"""
Build a double-clickable BlitzTrade launcher on the Desktop.
  - macOS  → ~/Desktop/BlitzTrade.app  (zero dependencies)
  - Windows → ~/Desktop/BlitzTrade.exe  (needs: pip install pyinstaller)

The app is self-contained — it points back to this repo directory,
so start.bat / start.sh don't need to be on the Desktop.

Usage:  python build_launcher.py
"""

import os, sys, struct, zlib, stat, shutil, platform, subprocess, math

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DESKTOP = os.path.join(os.path.expanduser('~'), 'Desktop')
os.chdir(SCRIPT_DIR)

# ───────────────────────────────────────────────────────────────
# 0.  Clean up any previous build artefacts in the repo folder
# ───────────────────────────────────────────────────────────────

def cleanup():
    """Remove all build artefacts from the repo directory."""
    print("🧹  Cleaning artefacts …")
    for name in (
        'Screener.app', 'BlitzTrade.app',
        'Screener.exe', 'BlitzTrade.exe',
        'screener.ico', 'blitz_trade.ico',
        '_launcher_tmp.py', 'Screener.spec', 'BlitzTrade.spec',
        'build', 'dist',
    ):
        p = os.path.join(SCRIPT_DIR, name)
        if os.path.isdir(p):
            shutil.rmtree(p)
            print(f"  removed {name}/")
        elif os.path.isfile(p):
            os.remove(p)
            print(f"  removed {name}")


# ───────────────────────────────────────────────────────────────
# 1.  Render the BlitzTrade favicon as RGBA pixels (no deps)
#     Matches: <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/>
#              stroke="#58a6ff" stroke-width="2" fill="none"
#     on #0d1117 rounded-rect background.
#     4× supersampling for clean anti-aliased edges.
# ───────────────────────────────────────────────────────────────

def _dist_to_segment(px, py, x0, y0, x1, y1):
    dx, dy = x1 - x0, y1 - y0
    len2 = dx * dx + dy * dy
    if len2 == 0:
        return math.hypot(px - x0, py - y0)
    t = max(0.0, min(1.0, ((px - x0) * dx + (py - y0) * dy) / len2))
    return math.hypot(px - (x0 + t * dx), py - (y0 + t * dy))


def _bolt_distance(px, py, bolt):
    """Shortest distance from (px, py) to any edge of the bolt polygon."""
    d = float('inf')
    n = len(bolt)
    for i in range(n):
        x0, y0 = bolt[i]
        x1, y1 = bolt[(i + 1) % n]
        d = min(d, _dist_to_segment(px, py, x0, y0, x1, y1))
    return d


def _sample(x, y, bolt, stroke_w, cx, cy, corner):
    """Return (r, g, b, a) for one sub-pixel sample."""
    # Rounded-rect mask
    dx = max(0.0, abs(x - cx) - (cx - corner))
    dy = max(0.0, abs(y - cy) - (cy - corner))
    if dx * dx + dy * dy > corner * corner:
        return (0, 0, 0, 0)

    # Bolt stroke (outline only, matching the favicon style)
    if _bolt_distance(x, y, bolt) <= stroke_w:
        return (0x58, 0xa6, 0xff, 0xff)  # #58a6ff

    # Background
    return (0x0d, 0x11, 0x17, 0xff)  # #0d1117


def draw_icon(size):
    """Render size×size RGBA: stroked BlitzTrade bolt on dark rounded rect."""
    pixels = bytearray(size * size * 4)

    # SVG path vertices in 24×24 viewBox
    bolt_svg = [(13, 2), (3, 14), (12, 14), (11, 22), (21, 10), (12, 10)]

    # Map to pixel coords with padding
    pad = size * 0.15
    inner = size - 2 * pad
    bolt = [(pad + bx / 24.0 * inner, pad + by / 24.0 * inner) for bx, by in bolt_svg]

    # Stroke width: SVG uses 2/24 of viewBox; scale to pixel size
    stroke_w = size * (2.0 / 24.0) * 0.55

    cx = cy = size / 2.0
    corner = size * 0.18

    SS = 4  # 4×4 supersampling
    step = 1.0 / SS
    start = step / 2.0

    for py in range(size):
        for px in range(size):
            rt, gt, bt, at = 0, 0, 0, 0
            for sy in range(SS):
                fy = py + start + sy * step
                for sx in range(SS):
                    fx = px + start + sx * step
                    r, g, b, a = _sample(fx, fy, bolt, stroke_w, cx, cy, corner)
                    rt += r; gt += g; bt += b; at += a
            n = SS * SS
            off = (py * size + px) * 4
            pixels[off] = rt // n
            pixels[off + 1] = gt // n
            pixels[off + 2] = bt // n
            pixels[off + 3] = at // n

    return bytes(pixels)


# ───────────────────────────────────────────────────────────────
# 2.  Encode RGBA → PNG (minimal writer, zero deps)
# ───────────────────────────────────────────────────────────────

def rgba_to_png(rgba, w, h):
    raw = b''
    stride = w * 4
    for y in range(h):
        raw += b'\x00' + rgba[y * stride:(y + 1) * stride]
    def chunk(ctype, data):
        c = ctype + data
        return struct.pack('>I', len(data)) + c + struct.pack('>I', zlib.crc32(c) & 0xffffffff)
    ihdr = struct.pack('>IIBBBBB', w, h, 8, 6, 0, 0, 0)
    return (b'\x89PNG\r\n\x1a\n'
            + chunk(b'IHDR', ihdr)
            + chunk(b'IDAT', zlib.compress(raw, 9))
            + chunk(b'IEND', b''))


# ───────────────────────────────────────────────────────────────
# 3.  .ico / .icns writers
# ───────────────────────────────────────────────────────────────

def make_ico(path):
    sizes = [256, 48, 32, 16]
    entries = []
    for s in sizes:
        print(f"  rendering {s}×{s} …")
        png = rgba_to_png(draw_icon(s), s, s)
        entries.append((s, png))
    hdr = struct.pack('<HHH', 0, 1, len(entries))
    offset = 6 + 16 * len(entries)
    dir_entries = img_data = b''
    for s, png in entries:
        w = 0 if s == 256 else s
        dir_entries += struct.pack('<BBBBHHII', w, w, 0, 0, 1, 32, len(png), offset)
        offset += len(png)
        img_data += png
    with open(path, 'wb') as f:
        f.write(hdr + dir_entries + img_data)
    print(f"  ✓ {os.path.basename(path)}")


def make_icns(path):
    entries = []
    for tag, s in [(b'ic08', 256), (b'ic07', 128)]:
        print(f"  rendering {s}×{s} …")
        png = rgba_to_png(draw_icon(s), s, s)
        entries.append((tag, png))
    body = b''
    for tag, png in entries:
        body += tag + struct.pack('>I', len(png) + 8) + png
    with open(path, 'wb') as f:
        f.write(b'icns' + struct.pack('>I', len(body) + 8) + body)
    print(f"  ✓ {os.path.basename(path)}")


# ───────────────────────────────────────────────────────────────
# 4a.  macOS → ~/Desktop/BlitzTrade.app
# ───────────────────────────────────────────────────────────────

def build_mac():
    print("\n🍎  Building BlitzTrade.app → Desktop …")
    app = os.path.join(DESKTOP, 'BlitzTrade.app')

    if os.path.exists(app):
        shutil.rmtree(app)

    contents = os.path.join(app, 'Contents')
    macos = os.path.join(contents, 'MacOS')
    res = os.path.join(contents, 'Resources')
    for d in (macos, res):
        os.makedirs(d, exist_ok=True)

    make_icns(os.path.join(res, 'icon.icns'))

    with open(os.path.join(contents, 'Info.plist'), 'w') as f:
        f.write("""\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>CFBundleName</key><string>BlitzTrade</string>
  <key>CFBundleDisplayName</key><string>BlitzTrade</string>
  <key>CFBundleIdentifier</key><string>com.blitztrade.launcher</string>
  <key>CFBundleVersion</key><string>1.0</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleExecutable</key><string>launcher</string>
  <key>CFBundleIconFile</key><string>icon</string>
</dict></plist>
""")

    launcher = os.path.join(macos, 'launcher')
    with open(launcher, 'w') as f:
        f.write(f"""\
#!/bin/bash
REPO="{SCRIPT_DIR}"
cd "$REPO"
exec bash "$REPO/start.sh"
""")
    os.chmod(launcher, os.stat(launcher).st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    print("  ✓ ~/Desktop/BlitzTrade.app  →  double-click to launch!")


# ───────────────────────────────────────────────────────────────
# 4b.  Windows → ~/Desktop/BlitzTrade.exe
# ───────────────────────────────────────────────────────────────

def build_windows():
    print("\n🪟  Building BlitzTrade.exe → Desktop …")

    ico_path = os.path.join(SCRIPT_DIR, 'blitz_trade.ico')
    make_ico(ico_path)

    launcher_py = os.path.join(SCRIPT_DIR, '_launcher_tmp.py')
    with open(launcher_py, 'w') as f:
        f.write(f'''\
import subprocess, os
REPO = r"{SCRIPT_DIR}"
os.chdir(REPO)
subprocess.Popen(["cmd", "/c", os.path.join(REPO, "start.bat")], cwd=REPO)
''')

    try:
        import PyInstaller.__main__
    except ImportError:
        print("  Installing pyinstaller …")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pyinstaller'])
        import PyInstaller.__main__

    PyInstaller.__main__.run([
        launcher_py,
        '--onefile', '--noconsole',
        '--name=BlitzTrade',
        f'--icon={ico_path}',
        f'--distpath={DESKTOP}',
        '--workpath=' + os.path.join(SCRIPT_DIR, 'build'),
        '--specpath=' + SCRIPT_DIR,
        '--clean', '-y',
    ])

    print("  ✓ ~/Desktop/BlitzTrade.exe  →  double-click to launch!")


# ───────────────────────────────────────────────────────────────
# 5.  Main
# ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    cleanup()
    if platform.system() == 'Darwin':
        build_mac()
    elif platform.system() == 'Windows':
        build_windows()
    else:
        print("Unsupported OS. Run on macOS or Windows.")
        sys.exit(1)
    cleanup()  # final sweep of build artefacts
    print("\nDone! 🚀")
