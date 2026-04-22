#!/usr/bin/env python3
"""
BlitzTrade — Desktop launcher.

Starts the TWS API bridge (serve.py) in a background thread,
then opens a native pywebview window. No browser needed.

Also handles auto-update checks on startup.
"""

import hashlib
import json
import os
import platform
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import asyncio
import tempfile
import threading
import time
import traceback
import urllib.request
import webbrowser
import zipfile


_early_log_lock = threading.Lock()
_early_log_initialized = False


def _early_log_path():
    app_data = os.path.join(
        os.environ.get("LOCALAPPDATA") or os.environ.get("HOME") or os.getcwd(),
        "BlitzTrade",
    )
    os.makedirs(app_data, exist_ok=True)
    return os.path.join(app_data, "launcher-bootstrap.log")


def _early_log(message):
    global _early_log_initialized
    try:
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")
        with _early_log_lock:
            mode = "w" if not _early_log_initialized else "a"
            with open(_early_log_path(), mode, encoding="utf-8") as f:
                f.write(f"[{stamp}] pid={os.getpid()} {message}\n")
            _early_log_initialized = True
    except Exception:
        pass


_early_log("launcher_module_import_start")

try:
    import webview
except Exception:
    _early_log(
        "launcher_module_import_webview_error: " + traceback.format_exc().strip()
    )
    raise
else:
    _early_log("launcher_module_import_webview_ok")

# ── Version ──────────────────────────────────────────────────
APP_VERSION = "2.0.0"

# Stamped to True by build_app.py at build time; False keeps auto-update disabled in dev.
# Set env var BLITZTRADE_IS_PACKAGED=1 locally to test the auto-update flow without a full build.
IS_PACKAGED = os.environ.get("BLITZTRADE_IS_PACKAGED") == "1"

# Update endpoint (API Gateway — injected at build time)
UPDATE_URL = os.environ.get(
    "BLITZ_UPDATE_URL",
    "",
)

# Optional, isolated IB Gateway setup helper (user-consented only)
IBGW_INSTALLER_URL = os.environ.get("BLITZ_IBGW_INSTALLER_URL", "").strip()
IBGW_DOWNLOAD_PAGE = os.environ.get(
    "BLITZ_IBGW_DOWNLOAD_PAGE",
    "https://www.interactivebrokers.com/en/trading/ibgateway-stable.php",
).strip()

# Cognito auth (stamped at build time by build_app.py; auto-fetched in dev)
COGNITO_REGION = "eu-central-1"
COGNITO_USER_POOL_ID = ""
COGNITO_CLIENT_ID = ""


def _app_data_dir():
    path = os.path.join(
        os.environ.get("LOCALAPPDATA") or os.environ.get("HOME") or str(Path.home()),
        "BlitzTrade",
    )
    os.makedirs(path, exist_ok=True)
    return path


def _startup_prefs_path():
    return os.path.join(_app_data_dir(), "startup_prefs.json")


def _load_startup_prefs():
    try:
        with open(_startup_prefs_path(), "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        return data
    except Exception:
        return {}


def _save_startup_prefs(conn_mode=None, auto_start_gw=None):
    prefs = _load_startup_prefs()
    if conn_mode is not None:
        prefs["connMode"] = "tws" if conn_mode == "tws" else "gateway"
    if auto_start_gw is not None:
        prefs["autoStartGW"] = bool(auto_start_gw)
    with open(_startup_prefs_path(), "w", encoding="utf-8") as f:
        json.dump(prefs, f)
    return prefs


def _should_auto_launch_ibgw():
    prefs = _load_startup_prefs()
    conn_mode = prefs.get("connMode", "gateway")
    auto_start_gw = bool(prefs.get("autoStartGW", False))
    return conn_mode == "gateway" and auto_start_gw


def _fetch_cognito_from_stack():
    """Auto-fetch Cognito IDs from CloudFormation when not stamped (dev mode)."""
    global COGNITO_USER_POOL_ID, COGNITO_CLIENT_ID
    if COGNITO_USER_POOL_ID and COGNITO_CLIENT_ID:
        return
    try:
        import json as _json

        raw = subprocess.check_output(
            [
                "aws",
                "cloudformation",
                "describe-stacks",
                "--stack-name",
                "blitztrade-web",
                "--region",
                COGNITO_REGION,
                "--query",
                "Stacks[0].Outputs",
                "--output",
                "json",
            ],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
        outputs = {o["OutputKey"]: o["OutputValue"] for o in _json.loads(raw)}
        COGNITO_USER_POOL_ID = outputs.get("UserPoolId", "")
        COGNITO_CLIENT_ID = outputs.get("DesktopClientId", "")
        if COGNITO_USER_POOL_ID:
            print(f"  Auto-fetched Cognito: pool={COGNITO_USER_POOL_ID}")
    except Exception:
        pass  # AWS CLI not available or not configured — skip silently


def _get_access_token():
    """Try to obtain a Cognito access token using a stored refresh token.

    Returns the access token string, or "" if unavailable.
    """
    if not COGNITO_CLIENT_ID or not COGNITO_REGION:
        return ""
    # Try to load stored refresh token (Windows DPAPI or plaintext)
    auth_dir = os.path.join(
        os.environ.get("LOCALAPPDATA") or os.environ.get("HOME") or str(Path.home()),
        "BlitzTrade",
    )
    refresh_file = os.path.join(auth_dir, "auth_refresh_token.bin")
    refresh_token = ""
    if os.path.exists(refresh_file):
        try:
            raw = open(refresh_file, "rb").read()
            if platform.system() == "Windows":
                # DPAPI decrypt
                import ctypes, ctypes.wintypes
                class _BLOB(ctypes.Structure):
                    _fields_ = [("cbData", ctypes.wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]
                inp = _BLOB(len(raw), ctypes.cast(ctypes.create_string_buffer(raw, len(raw)), ctypes.POINTER(ctypes.c_char)))
                out = _BLOB()
                if ctypes.windll.crypt32.CryptUnprotectData(ctypes.byref(inp), None, None, None, None, 0, ctypes.byref(out)):
                    refresh_token = ctypes.string_at(out.pbData, out.cbData).decode("utf-8", errors="ignore")
                    ctypes.windll.kernel32.LocalFree(out.pbData)
            else:
                refresh_token = raw.decode("utf-8", errors="ignore")
        except Exception:
            pass
    if not refresh_token:
        return ""
    # Call Cognito InitiateAuth to refresh
    try:
        url = f"https://cognito-idp.{COGNITO_REGION}.amazonaws.com/"
        payload = json.dumps({
            "AuthFlow": "REFRESH_TOKEN_AUTH",
            "ClientId": COGNITO_CLIENT_ID,
            "AuthParameters": {"REFRESH_TOKEN": refresh_token},
        }).encode()
        req = urllib.request.Request(
            url, data=payload, method="POST",
            headers={
                "Content-Type": "application/x-amz-json-1.1",
                "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        return data.get("AuthenticationResult", {}).get("AccessToken", "")
    except Exception:
        return ""


# ── Paths ────────────────────────────────────────────────────


def _base_dir():
    """Return the directory where bundled files live."""
    # PyInstaller sets sys._MEIPASS when running from a bundle
    if getattr(sys, "_MEIPASS", None):
        return sys._MEIPASS
    return os.path.dirname(os.path.abspath(__file__))


BASE_DIR = _base_dir()
_single_instance_handle = None
_log_lock = threading.Lock()
_log_initialized = False


def _is_packaged_runtime():
    return IS_PACKAGED


def _log_path():
    app_data = os.path.join(
        os.environ.get("LOCALAPPDATA") or os.environ.get("HOME") or BASE_DIR,
        "BlitzTrade",
    )
    os.makedirs(app_data, exist_ok=True)
    return os.path.join(app_data, "launcher.log")


def _log(message):
    global _log_initialized
    try:
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")
        line = (
            f"[{stamp}] pid={os.getpid()} tid={threading.get_ident()} " f"{message}\n"
        )
        with _log_lock:
            mode = "w" if not _log_initialized else "a"
            with open(_log_path(), mode, encoding="utf-8") as f:
                f.write(line)
            with open(_early_log_path(), "a", encoding="utf-8") as f:
                f.write(line)
            _log_initialized = True
        # Also print to stderr for debugging
        print(f"[LOG] {line.strip()}", file=sys.stderr)
    except Exception as e:
        print(f"[LOG_ERROR] {e}", file=sys.stderr)


def _prepare_windows_pythonnet():
    if platform.system() != "Windows":
        return

    runtime_dir = os.path.join(BASE_DIR, "pythonnet", "runtime")
    arch_dir = "amd64" if sys.maxsize > 2**32 else "x86"
    clr_loader_dir = os.path.join(BASE_DIR, "clr_loader", "ffi", "dlls", arch_dir)

    for candidate in (BASE_DIR, runtime_dir, clr_loader_dir):
        if not os.path.isdir(candidate):
            continue
        os.environ["PATH"] = candidate + os.pathsep + os.environ.get("PATH", "")
        try:
            os.add_dll_directory(candidate)
        except (AttributeError, FileNotFoundError, OSError):
            pass

    os.environ.setdefault("PYTHONNET_RUNTIME", "netfx")

    try:
        import pythonnet

        pythonnet.load("netfx")

        import clr

        clr.AddReference("System.Windows.Forms")
        _log("launcher_pythonnet_netfx_ready")
    except Exception:
        _log("launcher_pythonnet_netfx_error: " + traceback.format_exc().strip())


def _prepare_windows_pywebview_shims():
    if platform.system() != "Windows":
        return

    try:
        import webview.platforms as webview_platforms
        import pywebview_win32_shim as win32_shim

        sys.modules["webview.platforms.win32"] = win32_shim
        setattr(webview_platforms, "win32", win32_shim)
        _log("launcher_pywebview_win32_shim_ready")
    except Exception:
        _log("launcher_pywebview_win32_shim_error: " + traceback.format_exc().strip())


def _install_global_excepthook():
    def _hook(exc_type, exc_value, exc_tb):
        text = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        _log("unhandled_exception: " + text.strip())

    sys.excepthook = _hook


# ── Utility ──────────────────────────────────────────────────


def _find_free_port():
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_server(port, timeout=15):
    """Block until the aiohttp server is accepting connections."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def _apply_windows_window_icon(window_title, icon_path, timeout=15):
    """Set the native window icon on Windows title bars once the webview window exists."""
    if platform.system() != "Windows" or not os.path.exists(icon_path):
        return
    try:
        import ctypes
        from ctypes import wintypes

        user32 = ctypes.windll.user32

        # Declare proper types so 64-bit handles are not truncated
        user32.FindWindowW.restype = wintypes.HWND
        user32.FindWindowW.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR]
        user32.LoadImageW.restype = wintypes.HANDLE
        user32.LoadImageW.argtypes = [
            wintypes.HINSTANCE,
            wintypes.LPCWSTR,
            wintypes.UINT,
            ctypes.c_int,
            ctypes.c_int,
            wintypes.UINT,
        ]
        user32.SendMessageW.restype = ctypes.c_ssize_t  # LRESULT (pointer-sized)
        user32.SendMessageW.argtypes = [
            wintypes.HWND,
            wintypes.UINT,
            wintypes.WPARAM,
            wintypes.LPARAM,
        ]

        WM_SETICON = 0x0080
        ICON_SMALL = 0
        ICON_BIG = 1
        IMAGE_ICON = 1
        LR_LOADFROMFILE = 0x0010

        hwnd = None
        deadline = time.time() + timeout
        while time.time() < deadline:
            hwnd = user32.FindWindowW(None, window_title)
            if hwnd:
                break
            time.sleep(0.2)
        if not hwnd:
            return

        hicon = user32.LoadImageW(None, icon_path, IMAGE_ICON, 0, 0, LR_LOADFROMFILE)
        if not hicon:
            return
        user32.SendMessageW(hwnd, WM_SETICON, ICON_SMALL, hicon)
        user32.SendMessageW(hwnd, WM_SETICON, ICON_BIG, hicon)
    except Exception:
        pass


def _focus_existing_windows_instance(window_title):
    if platform.system() != "Windows":
        return False
    try:
        import ctypes

        user32 = ctypes.windll.user32
        hwnd = user32.FindWindowW(None, window_title)
        if not hwnd:
            return False
        SW_RESTORE = 9
        user32.ShowWindow(hwnd, SW_RESTORE)
        user32.SetForegroundWindow(hwnd)
        return True
    except Exception:
        return False


def _ensure_single_instance(window_title):
    global _single_instance_handle
    if platform.system() != "Windows":
        return True
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        ERROR_ALREADY_EXISTS = 183
        mutex_name = "Local\\BlitzTrade.SingleInstance"
        handle = kernel32.CreateMutexW(None, False, mutex_name)
        if not handle:
            return True
        _single_instance_handle = handle
        if kernel32.GetLastError() == ERROR_ALREADY_EXISTS:
            _focus_existing_windows_instance(window_title)
            return False
        return True
    except Exception:
        return True


# ── Auto-Update ──────────────────────────────────────────────

_update_info = None  # set by check thread: {"version": "1.2.0", "url": "..."}
_update_status = {"state": "idle", "progress": 0, "message": "", "error": ""}
_update_status_lock = threading.Lock()

_ibgw_status = {
    "state": "idle",
    "progress": 0,
    "message": "",
    "error": "",
    "installed": False,
    "running": False,
}
_ibgw_status_lock = threading.Lock()


def _set_update_status(state=None, progress=None, message=None, error=None):
    with _update_status_lock:
        if state is not None:
            _update_status["state"] = state
        if progress is not None:
            _update_status["progress"] = progress
        if message is not None:
            _update_status["message"] = message
        if error is not None:
            _update_status["error"] = error


def _get_update_status():
    with _update_status_lock:
        s = dict(_update_status)
    if _update_info and not s.get("version"):
        s["version"] = _update_info.get("version", "")
    return s


def _set_ibgw_status(state=None, progress=None, message=None, error=None):
    with _ibgw_status_lock:
        if state is not None:
            _ibgw_status["state"] = state
        if progress is not None:
            _ibgw_status["progress"] = progress
        if message is not None:
            _ibgw_status["message"] = message
        if error is not None:
            _ibgw_status["error"] = error


def _is_port_open(port):
    try:
        with socket.create_connection(("127.0.0.1", int(port)), timeout=0.5):
            return True
    except Exception:
        return False


def _is_ibgw_running():
    if platform.system() != "Windows":
        return False
    try:
        out = subprocess.check_output(
            ["tasklist", "/fo", "csv", "/nh"],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=5,
        ).lower()
        if "ibgateway" in out:
            return True
    except Exception:
        pass
    # Fallback signal: default Gateway API ports listening
    return _is_port_open(4001) or _is_port_open(4002)


def _is_ibgw_installed_windows():
    candidates = []
    for base in [os.environ.get("PROGRAMFILES"), os.environ.get("PROGRAMFILES(X86)")]:
        if not base:
            continue
        candidates.extend(
            [
                os.path.join(base, "IB Gateway"),
                os.path.join(base, "IBKR", "IB Gateway"),
            ]
        )
    candidates.extend(
        [
            r"C:\Jts",
            r"C:\IBGateway",
        ]
    )
    for p in candidates:
        try:
            if os.path.isdir(p):
                low = p.lower()
                if "gateway" in low or low.endswith("\\jts"):
                    return True
        except Exception:
            pass

    # Registry fallback
    try:
        import winreg

        roots = [
            (
                winreg.HKEY_LOCAL_MACHINE,
                r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall",
            ),
            (
                winreg.HKEY_LOCAL_MACHINE,
                r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall",
            ),
            (
                winreg.HKEY_CURRENT_USER,
                r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall",
            ),
        ]
        for root, key_path in roots:
            try:
                key = winreg.OpenKey(root, key_path)
            except Exception:
                continue
            count = winreg.QueryInfoKey(key)[0]
            for i in range(count):
                try:
                    sub_name = winreg.EnumKey(key, i)
                    sub = winreg.OpenKey(key, sub_name)
                    disp, _ = winreg.QueryValueEx(sub, "DisplayName")
                    if (
                        isinstance(disp, str)
                        and "gateway" in disp.lower()
                        and "interactive brokers" in disp.lower()
                    ):
                        return True
                except Exception:
                    continue
    except Exception:
        pass
    return False


def _detect_ibgw_state():
    installed = platform.system() == "Windows" and _is_ibgw_installed_windows()
    running = _is_ibgw_running()
    return installed, running


def _download_file_with_progress(url, out_path, progress_start=0, progress_end=70):
    req = urllib.request.Request(
        url, headers={"User-Agent": "BlitzTrade/" + APP_VERSION}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        total = int(resp.headers.get("Content-Length", "0") or 0)
        done = 0
        with open(out_path, "wb") as f:
            while True:
                chunk = resp.read(64 * 1024)
                if not chunk:
                    break
                f.write(chunk)
                done += len(chunk)
                if total > 0:
                    frac = done / total
                    p = int(progress_start + (progress_end - progress_start) * frac)
                    _set_ibgw_status(progress=max(0, min(100, p)))


def _ibgw_setup_worker():
    try:
        _set_ibgw_status(
            state="checking",
            progress=5,
            message="Checking IB Gateway status...",
            error="",
        )
        installed, running = _detect_ibgw_state()
        with _ibgw_status_lock:
            _ibgw_status["installed"] = installed
            _ibgw_status["running"] = running

        if running:
            _set_ibgw_status(
                state="completed",
                progress=100,
                message="IB Gateway is already running.",
                error="",
            )
            return

        if not installed:
            if not IBGW_INSTALLER_URL:
                _set_ibgw_status(
                    state="error",
                    progress=0,
                    message="",
                    error="Gateway installer URL is not configured.",
                )
                return

            _set_ibgw_status(
                state="downloading",
                progress=10,
                message="Downloading IB Gateway installer...",
                error="",
            )
            tmp_dir = tempfile.mkdtemp(prefix="blitztrade-ibgw-")
            ext = ".msi" if IBGW_INSTALLER_URL.lower().endswith(".msi") else ".exe"
            installer_path = os.path.join(tmp_dir, "ibgw-installer" + ext)
            _download_file_with_progress(
                IBGW_INSTALLER_URL, installer_path, progress_start=10, progress_end=70
            )

            _set_ibgw_status(
                state="installing",
                progress=80,
                message="Launching IB Gateway installer...",
                error="",
            )
            if ext == ".msi":
                proc = subprocess.Popen(["msiexec", "/i", installer_path])
            else:
                proc = subprocess.Popen([installer_path])
            proc.wait(timeout=3600)

        _set_ibgw_status(
            state="verifying",
            progress=92,
            message="Verifying installation...",
            error="",
        )
        for _ in range(120):
            installed, running = _detect_ibgw_state()
            with _ibgw_status_lock:
                _ibgw_status["installed"] = installed
                _ibgw_status["running"] = running
            if installed:
                _set_ibgw_status(
                    state="completed",
                    progress=100,
                    message="IB Gateway installed successfully.",
                    error="",
                )
                return
            time.sleep(2)

        _set_ibgw_status(
            state="error",
            progress=0,
            message="",
            error="Installer finished, but Gateway was not detected.",
        )
    except subprocess.TimeoutExpired:
        _set_ibgw_status(
            state="error",
            progress=0,
            message="",
            error="Installer timed out. Please try again.",
        )
    except Exception as e:
        _log("ibgw_setup_error: " + traceback.format_exc().strip())
        _set_ibgw_status(state="error", progress=0, message="", error=str(e))


def _find_ibgw_exe():
    """Search common install locations for the IB Gateway executable on Windows."""
    if platform.system() != "Windows":
        return None

    # C:\Jts\ibgateway\<version>\ — most common install location
    jts_gw = r"C:\Jts\ibgateway"
    if os.path.isdir(jts_gw):
        try:
            versions = sorted(
                (e for e in os.scandir(jts_gw) if e.is_dir()),
                key=lambda e: e.name,
                reverse=True,
            )
            for entry in versions:
                candidate = os.path.join(entry.path, "ibgateway.exe")
                if os.path.isfile(candidate):
                    return candidate
        except Exception:
            pass

    # Program Files variants
    for base in [os.environ.get("PROGRAMFILES"), os.environ.get("PROGRAMFILES(X86)")]:
        if not base:
            continue
        for subdir in ["IB Gateway", os.path.join("IBKR", "IB Gateway")]:
            candidate = os.path.join(base, subdir, "ibgateway.exe")
            if os.path.isfile(candidate):
                return candidate

    # Registry: InstallLocation from uninstall key
    try:
        import winreg

        roots = [
            (
                winreg.HKEY_LOCAL_MACHINE,
                r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall",
            ),
            (
                winreg.HKEY_LOCAL_MACHINE,
                r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall",
            ),
            (
                winreg.HKEY_CURRENT_USER,
                r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall",
            ),
        ]
        for root, key_path in roots:
            try:
                key = winreg.OpenKey(root, key_path)
            except Exception:
                continue
            count = winreg.QueryInfoKey(key)[0]
            for i in range(count):
                try:
                    sub_name = winreg.EnumKey(key, i)
                    sub = winreg.OpenKey(key, sub_name)
                    disp, _ = winreg.QueryValueEx(sub, "DisplayName")
                    if (
                        isinstance(disp, str)
                        and "gateway" in disp.lower()
                        and "interactive brokers" in disp.lower()
                    ):
                        try:
                            loc, _ = winreg.QueryValueEx(sub, "InstallLocation")
                            candidate = os.path.join(loc, "ibgateway.exe")
                            if os.path.isfile(candidate):
                                return candidate
                        except Exception:
                            pass
                except Exception:
                    continue
    except Exception:
        pass

    return None


def _ensure_ibgw_jts_ini_api_mode():
    """Write/patch jts.ini to pre-select 'IB API' on the Gateway login screen and
    suppress most interactive dialogs so Gateway runs as quietly as possible."""
    candidates = []
    # Prefer the jts.ini next to the detected ibgateway.exe (e.g. C:\Jts\ibgateway\1037\jts.ini)
    exe = _find_ibgw_exe()
    if exe:
        exe_dir = os.path.dirname(exe)
        candidates.extend(
            [
                os.path.join(exe_dir, "jts.ini"),
                os.path.join(os.path.dirname(exe_dir), "jts.ini"),
            ]
        )

    candidates.extend(
        [
            os.path.join(r"C:\Jts", "jts.ini"),
            os.path.join(os.path.expanduser("~"), "Jts", "jts.ini"),
        ]
    )

    # De-duplicate while preserving order
    uniq_candidates = []
    for c in candidates:
        if c and c not in uniq_candidates:
            uniq_candidates.append(c)
    candidates = uniq_candidates

    ini_path = None
    for c in candidates:
        if os.path.isfile(c):
            ini_path = c
            break
    if ini_path is None:
        ini_path = candidates[0]
        os.makedirs(os.path.dirname(ini_path), exist_ok=True)

    try:
        import configparser

        # Some real-world jts.ini files contain duplicate keys. Be tolerant so
        # we can still normalize values instead of failing with DuplicateOptionError.
        cfg = configparser.RawConfigParser(strict=False)
        cfg.optionxform = str  # preserve case
        if os.path.isfile(ini_path):
            cfg.read(ini_path, encoding="utf-8")
        if not cfg.has_section("IBGateway"):
            cfg.add_section("IBGateway")
        if not cfg.has_section("Logon"):
            cfg.add_section("Logon")
        # Show IB API tab by default (not FIX-CTCI)
        cfg.set("IBGateway", "ApiOnly", "true")
        cfg.set("Logon", "ApiOnly", "true")
        # Auto-accept incoming API connection requests (no popup dialog)
        cfg.set("IBGateway", "AcceptIncomingConnectionAction", "accept")
        cfg.set("Logon", "AcceptIncomingConnectionAction", "accept")
        # Suppress non-brokerage account warning
        cfg.set("IBGateway", "AcceptNonBrokerageAccountWarning", "true")
        cfg.set("Logon", "AcceptNonBrokerageAccountWarning", "true")
        # Suppress "save settings?" prompt on exit
        cfg.set("IBGateway", "isSaveAlreadyRequestedOnce", "true")
        cfg.set("Logon", "isSaveAlreadyRequestedOnce", "true")
        with open(ini_path, "w", encoding="utf-8") as f:
            cfg.write(f)
        _log(f"auto_launch_ibgw: patched jts.ini at {ini_path}")
    except Exception:
        _log("auto_launch_ibgw_jts_ini_error: " + traceback.format_exc().strip())


_ibgw_launch_state = {"launched": False, "awaiting_login": False}
_ibgw_launch_state_lock = threading.Lock()


def _set_ibgw_launch_state(**kwargs):
    with _ibgw_launch_state_lock:
        _ibgw_launch_state.update(kwargs)


def _launch_ibgw(show_minimized=False):
    """Launch IB Gateway executable if present.

    Returns True if launch was initiated, False otherwise.
    """
    exe = _find_ibgw_exe()
    if not exe:
        _log("launch_ibgw: executable not found")
        return False

    _ensure_ibgw_jts_ini_api_mode()

    kwargs = {"cwd": os.path.dirname(exe)}
    if show_minimized and platform.system() == "Windows":
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        si.wShowWindow = 2  # SW_SHOWMINIMIZED
        kwargs["startupinfo"] = si

    subprocess.Popen([exe], **kwargs)
    _log(f"launch_ibgw: started {exe} minimized={show_minimized}")
    return True


def _auto_launch_ibgw_if_needed():
    """If IB Gateway is installed but not running, launch it minimized on startup."""
    if platform.system() != "Windows":
        return
    try:
        installed, running = _detect_ibgw_state()
        if running:
            _log("auto_launch_ibgw: already running")
            return
        if not installed:
            _log("auto_launch_ibgw: not installed, skipping")
            return

        if not _find_ibgw_exe():
            _log("auto_launch_ibgw: executable not found in known locations")
            return

        if not _launch_ibgw(show_minimized=True):
            return
        _set_ibgw_launch_state(launched=True, awaiting_login=True)

        # Watch for Gateway to start accepting connections; clear awaiting_login flag
        def _wait_for_gateway():
            deadline = time.time() + 180  # give user 3 minutes to log in
            while time.time() < deadline:
                if _is_port_open(4001) or _is_port_open(4002):
                    _set_ibgw_launch_state(awaiting_login=False)
                    _log("auto_launch_ibgw: gateway port is open, login detected")
                    return
                time.sleep(3)
            _set_ibgw_launch_state(awaiting_login=False)  # timeout
            _log("auto_launch_ibgw: timed out waiting for login")

        threading.Thread(target=_wait_for_gateway, daemon=True).start()
    except Exception:
        _log("auto_launch_ibgw_error: " + traceback.format_exc().strip())


def _get_ibgw_status():
    installed, running = _detect_ibgw_state()
    with _ibgw_status_lock:
        s = dict(_ibgw_status)
        s["installed"] = installed
        s["running"] = running
    if s["state"] == "idle" and running:
        s["message"] = "IB Gateway is running."
    return s


def _check_for_updates():
    """Query the Lambda endpoint for the latest version and auto-install."""
    global _update_info
    if not UPDATE_URL:
        _log("update_check_skipped: UPDATE_URL not set")
        return
    try:
        url = (
            UPDATE_URL.rstrip("/")
            + f"/update?v={APP_VERSION}&platform={_platform_key()}"
        )
        _log(f"update_check_start: url={url}")
        headers = {"User-Agent": "BlitzTrade/" + APP_VERSION}
        token = _get_access_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        _log(
            "update_check_result: "
            f"available={bool(data.get('update_available'))} "
            f"latest={data.get('latest_version')}"
        )

        # Check min_version constraint
        min_version = data.get("min_version")
        if min_version and _version_lt(APP_VERSION, min_version):
            _log(f"update_blocked: current {APP_VERSION} < min_version {min_version}")
            return

        if data.get("update_available"):
            _update_info = {
                "version": data["latest_version"],
                "url": data["download_url"],
                "sha256": data.get("sha256", ""),
                "released_at": data.get("released_at"),
                "summary": data.get("summary", ""),
                "release_notes": data.get("release_notes") or [],
            }
            _log(
                f"update_available: v{_update_info['version']} (awaiting user login to auto-install)"
            )
    except Exception:
        _log("update_check_error: " + traceback.format_exc().strip())
        pass  # silent — don't block the app


def _platform_key():
    s = platform.system()
    if s == "Darwin":
        return "mac"
    if s == "Windows":
        return "win"
    return "linux"


def _version_lt(a, b):
    """Return True if version a < version b (semantic versioning)."""

    def parts(v):
        try:
            return [int(x) for x in v.split(".")[:3]]
        except (ValueError, AttributeError):
            return [0, 0, 0]

    return parts(a) < parts(b)


def _current_exe_for_update():
    """Return the on-disk launcher path to replace during self-update."""
    compiled_meta = globals().get("__compiled__")
    original_argv0 = getattr(compiled_meta, "original_argv0", None)
    if original_argv0:
        return os.path.abspath(original_argv0)
    if sys.argv and sys.argv[0]:
        return os.path.abspath(sys.argv[0])
    return sys.executable


def _download_with_integrity(url, expected_hash, version):
    """Download file with SHA256 verification and retry logic."""
    max_retries = 3
    for attempt in range(max_retries):
        tmp_fd = None
        tmp_path = None
        try:
            tmp_fd, tmp_path = tempfile.mkstemp(
                suffix=".zip" if url.split("?")[0].lower().endswith(".zip") else ""
            )
            os.close(tmp_fd)
            tmp_fd = None

            req = urllib.request.Request(
                url, headers={"User-Agent": "BlitzTrade/" + APP_VERSION}
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                total_bytes = int(resp.headers.get("Content-Length") or 0)
                if total_bytes == 0:
                    raise ValueError("Server returned no Content-Length")

                # Stream and hash simultaneously
                hasher = hashlib.sha256()
                downloaded = 0
                with open(tmp_path, "wb") as f:
                    while True:
                        chunk = resp.read(1024 * 256)
                        if not chunk:
                            break
                        hasher.update(chunk)
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_bytes > 0:
                            progress = min(95, int(downloaded * 100 / total_bytes))
                            _set_update_status(progress=progress)

            if downloaded == 0:
                raise RuntimeError("Downloaded file is empty")

            # Verify hash
            actual_hash = hasher.hexdigest()
            if actual_hash != expected_hash:
                _log(
                    f"update_hash_mismatch: expected={expected_hash} actual={actual_hash}"
                )
                raise ValueError(f"File corrupted: SHA256 mismatch")

            _log(f"update_download_done: bytes={downloaded} hash_verified")
            return tmp_path  # success

        except Exception as e:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            if attempt < max_retries - 1:
                _log(f"update_download_retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(2**attempt)  # exponential backoff
            else:
                raise
        finally:
            if tmp_fd is not None:
                try:
                    os.close(tmp_fd)
                except Exception:
                    pass


def _safe_binary_replace_windows(old_exe, tmp_exe):
    """Atomic Windows replacement with rollback on failure."""
    backup = old_exe + ".bak"
    replace_log = os.path.join(os.path.dirname(_log_path()), "update-replace.log")

    try:
        # Create backup FIRST (for rollback)
        if os.path.exists(backup):
            os.remove(backup)
        shutil.copy2(old_exe, backup)
        _log("update_backup_created")
    except Exception as e:
        _log(f"update_backup_failed: {e}")
        raise RuntimeError("Cannot create backup for rollback")

    try:
        # Write replacement script with retries because the current process may
        # not release the EXE lock immediately after spawning the helper.
        bat = tmp_exe + ".bat"
        with open(bat, "w") as f:
            f.write(
                f"@echo off\n"
                f"setlocal enabledelayedexpansion\n"
                f'set "LOG={replace_log}"\n'
                f'echo [%date% %time%] helper_start old_exe={old_exe} tmp_exe={tmp_exe}>>"%LOG%"\n'
                f"set /a tries=0\n"
                f":retry_move\n"
                f"set /a tries+=1\n"
                f'move /y "{tmp_exe}" "{old_exe}" >nul 2>&1\n'
                f'if not exist "{tmp_exe}" goto move_ok\n'
                f'echo [%date% %time%] move_retry !tries!>>"%LOG%"\n'
                f"if !tries! geq 30 goto move_fail\n"
                f"timeout /t 1 /nobreak >nul\n"
                f"goto retry_move\n"
                f":move_ok\n"
                f'echo [%date% %time%] move_ok>>"%LOG%"\n'
                f'if not exist "{old_exe}" goto move_fail\n'
                f'del "{backup}" >nul 2>&1\n'
                f'echo [%date% %time%] starting_new_exe>>"%LOG%"\n'
                f'start "" "{old_exe}"\n'
                f'echo [%date% %time%] helper_done>>"%LOG%"\n'
                f'(goto) 2>nul & del "%~f0"\n'
                f"exit /b 0\n"
                f":move_fail\n"
                f'echo [%date% %time%] move_failed>>"%LOG%"\n'
                f'if exist "{backup}" move /y "{backup}" "{old_exe}" >nul 2>&1\n'
                f'echo [%date% %time%] rollback_attempted>>"%LOG%"\n'
                f"exit /b 1\n"
            )
        _log(f"update_bat_created: {bat}")

        subprocess.Popen(["cmd", "/c", bat], creationflags=0x08000000)
        _log("update_windows_process_started")

    except Exception as e:
        # Restore from backup on any error
        try:
            if os.path.exists(backup):
                shutil.copy2(backup, old_exe)
            _log(f"update_rollback_restored")
        except Exception as rollback_err:
            _log(f"update_rollback_failed: {rollback_err}")
        raise


def _safe_binary_replace_unix(old_exe, tmp_file, is_zip):
    """Atomic Unix replacement with rollback."""
    backup = old_exe + ".bak"

    try:
        # Create backup FIRST
        if os.path.exists(backup):
            os.remove(backup)
        shutil.copy2(old_exe, backup)
        _log("update_backup_created")
    except Exception as e:
        _log(f"update_backup_failed: {e}")
        raise RuntimeError("Cannot create backup for rollback")

    try:
        if is_zip:
            extract_dir = tmp_file + "_extracted"
            with zipfile.ZipFile(tmp_file, "r") as zf:
                zf.extractall(extract_dir)
            # Find the binary (should be "BlitzTrade")
            extracted = os.path.join(extract_dir, "BlitzTrade")
            if not os.path.exists(extracted):
                # Fallback: first file in the zip
                names = os.listdir(extract_dir)
                if names:
                    extracted = os.path.join(extract_dir, names[0])
                else:
                    raise RuntimeError("Empty zip archive")
            os.chmod(extracted, 0o755)
            os.replace(extracted, old_exe)
            shutil.rmtree(extract_dir, ignore_errors=True)
            _log("update_unix_zip_installed")
        else:
            os.chmod(tmp_file, 0o755)
            os.replace(tmp_file, old_exe)
            _log("update_unix_binary_installed")

        # Clean up backup after successful replacement
        if os.path.exists(backup):
            os.remove(backup)

        # Spawn new instance
        subprocess.Popen([old_exe])
        _log("update_unix_process_started")

    except Exception as e:
        # Restore from backup
        try:
            if os.path.exists(backup):
                shutil.copy2(backup, old_exe)
            _log("update_rollback_restored")
        except Exception as rollback_err:
            _log(f"update_rollback_failed: {rollback_err}")
        raise


def _fresh_download_url():
    """Re-fetch a fresh pre-signed download URL from the Lambda (the one from
    startup may have expired — S3 pre-signed URLs only last 5 minutes)."""
    if not UPDATE_URL:
        return None
    try:
        url = (
            UPDATE_URL.rstrip("/")
            + f"/update?v={APP_VERSION}&platform={_platform_key()}"
        )
        headers = {"User-Agent": "BlitzTrade/" + APP_VERSION}
        token = _get_access_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        if data.get("update_available") and data.get("download_url"):
            return data["download_url"]
    except Exception:
        pass
    return None


def _download_and_replace(update_info):
    """Download the new binary with integrity checks and replace current executable."""
    try:
        url = update_info.get("url") if isinstance(update_info, dict) else update_info
        version = update_info.get("version") if isinstance(update_info, dict) else ""
        expected_hash = (
            update_info.get("sha256", "") if isinstance(update_info, dict) else ""
        )

        _log(
            f"update_install_begin: target_version={version or 'unknown'} "
            f"hash_provided={bool(expected_hash)}"
        )

        if not expected_hash:
            _set_update_status(
                state="error",
                progress=0,
                message="",
                error="Update package missing integrity hash.",
            )
            _log("update_install_error: no sha256 hash provided")
            return

        # Under Nuitka onefile, sys.executable is the extracted interpreter inside
        # the temp extraction dir, not the actual BlitzTrade.exe launcher. Use
        # Nuitka's original argv[0] metadata so replacement targets the real EXE.
        current_exe = _current_exe_for_update()
        if not _is_packaged_runtime():
            _set_update_status(
                state="error",
                progress=0,
                message="",
                error="Auto-update only available in packaged app.",
            )
            _log("update_install_skipped_not_packaged")
            return

        # Always get a fresh pre-signed URL
        try:
            fresh_url = _fresh_download_url()
            if fresh_url:
                url = fresh_url
                _log("update_install_refreshed_presigned_url")
        except Exception as e:
            _log(f"update_install_presigned_refresh_failed: {e}")
            pass  # use original URL

        # Download with integrity verification
        _set_update_status(
            state="downloading",
            progress=0,
            message=(
                f"Downloading v{version}..." if version else "Downloading update..."
            ),
        )

        tmp_file = _download_with_integrity(url, expected_hash, version)

        _set_update_status(
            state="installing", progress=96, message="Installing update..."
        )

        # Detect if the download is a zip
        url_path = url.split("?")[0] if "?" in url else url
        is_zip = url_path.lower().endswith(".zip")

        # Platform-specific atomic replacement with rollback
        if platform.system() == "Windows":
            _safe_binary_replace_windows(current_exe, tmp_file)
        else:
            _safe_binary_replace_unix(current_exe, tmp_file, is_zip)

        _set_update_status(
            state="restarting", progress=100, message="Restarting BlitzTrade..."
        )
        _log("update_install_exiting_current_process")
        os._exit(0)

    except Exception as e:
        print(f"[update] Failed: {e}")
        _log(f"update_install_error: {e}")
        _set_update_status(state="error", progress=0, message="", error=str(e))


# ── JS API exposed to the webview ────────────────────────────


class BlitzAPI:
    """Methods callable from JavaScript via pywebview.api.*"""

    def get_version(self):
        return APP_VERSION

    def get_update_info(self):
        """Returns None or {"version": "x.y.z", "url": "..."}"""
        return _update_info

    def get_update_status(self):
        return _get_update_status()

    def check_for_updates(self):
        """Called by the JS frontend after the user logs in.
        Re-checks for an update (in case the metadata fetch hasn't finished yet)
        and immediately triggers auto-install if one is available."""

        def _run():
            _check_for_updates()  # refresh metadata
            if _update_info and _update_info.get("url") and _is_packaged_runtime():
                status = _get_update_status()
                if status["state"] not in {"downloading", "installing", "restarting"}:
                    _log("update_install_triggered_post_login")
                    _set_update_status(
                        state="downloading",
                        progress=0,
                        message=(
                            f"Downloading v{_update_info['version']}..."
                            if _update_info.get("version")
                            else "Downloading update..."
                        ),
                        error="",
                    )
                    _download_and_replace(_update_info)

        threading.Thread(target=_run, daemon=True).start()

    def install_update(self):
        """Download and install the pending update, then restart."""
        status = _get_update_status()
        if status["state"] in {"downloading", "installing", "restarting"}:
            return False
        if _update_info and _update_info.get("url"):
            threading.Thread(
                target=_download_and_replace,
                args=(_update_info,),
                daemon=True,
            ).start()
            return True
        return False

    def get_ibgw_setup_status(self):
        return _get_ibgw_status()

    def get_ibgw_launch_state(self):
        """Return whether Gateway was auto-launched this session and if it's awaiting login."""
        with _ibgw_launch_state_lock:
            return dict(_ibgw_launch_state)

    def launch_ibgw(self):
        """Trigger the same IB Gateway auto-launch routine used on startup."""
        if platform.system() != "Windows":
            _set_ibgw_status(
                state="error",
                progress=0,
                message="",
                error="IB Gateway launch is currently supported on Windows only.",
            )
            return False

        installed, running = _detect_ibgw_state()
        if running:
            _set_ibgw_status(
                state="completed",
                progress=100,
                message="IB Gateway is already running.",
                error="",
            )
            return True
        if not installed:
            _set_ibgw_status(
                state="error",
                progress=0,
                message="",
                error="IB Gateway is not installed.",
            )
            return False

        _set_ibgw_status(
            state="checking",
            progress=10,
            message="Launching IB Gateway...",
            error="",
        )
        _auto_launch_ibgw_if_needed()
        _set_ibgw_status(
            state="checking",
            progress=20,
            message="IB Gateway launched. Please log in.",
            error="",
        )
        return True

    def begin_ibgw_setup(self, consent=False):
        if platform.system() != "Windows":
            _set_ibgw_status(
                state="error",
                progress=0,
                message="",
                error="IB Gateway guided install is currently supported on Windows only.",
            )
            return False
        if not consent:
            _set_ibgw_status(
                state="error",
                progress=0,
                message="",
                error="Consent is required before installing IB Gateway.",
            )
            return False

        st = _get_ibgw_status()
        if st.get("state") in {"checking", "downloading", "installing", "verifying"}:
            return False

        threading.Thread(target=_ibgw_setup_worker, daemon=True).start()
        return True

    def open_ibgw_download_page(self):
        try:
            webbrowser.open(IBGW_DOWNLOAD_PAGE)
            return True
        except Exception:
            return False

    def open_external_url(self, url):
        try:
            if not isinstance(url, str):
                return False
            url = url.strip()
            if not url.startswith(("http://", "https://")):
                return False
            webbrowser.open(url)
            return True
        except Exception:
            return False

    def save_startup_preferences(self, conn_mode=None, auto_start_gw=None):
        try:
            return _save_startup_prefs(conn_mode=conn_mode, auto_start_gw=auto_start_gw)
        except Exception:
            _log("save_startup_preferences_error: " + traceback.format_exc().strip())
            return False

    def get_startup_preferences(self):
        try:
            return _load_startup_prefs()
        except Exception:
            return {}

    def save_image(self, data_b64, default_name="image.png"):
        """Save a base64-encoded PNG via native file dialog."""
        import base64

        try:
            windows = webview.windows
            if not windows:
                return {"error": "No window"}
            result = windows[0].create_file_dialog(
                webview.SAVE_DIALOG,
                save_filename=default_name,
                file_types=("PNG Files (*.png)",),
            )
            if not result:
                return {"cancelled": True}
            path = result if isinstance(result, str) else result[0]
            raw = base64.b64decode(data_b64)
            with open(path, "wb") as f:
                f.write(raw)
            return {"ok": True, "path": path}
        except Exception as e:
            return {"error": str(e)}


# ── Server Thread ────────────────────────────────────────────


_SERVER_MAX_RESTARTS = 20
_SERVER_RESTART_DELAY = 3
_SERVER_COOLDOWN = 60  # reset restart counter after this many seconds of uptime


def _start_server(port, tws_port=None):
    """Import and run serve.py in a thread with watchdog auto-restart."""
    restarts = 0
    _log(f"server_thread_start: port={port} tws_port={tws_port}")
    while restarts < _SERVER_MAX_RESTARTS:
        # Python 3.10+ doesn't auto-create an event loop for non-main threads.
        # Must set one BEFORE importing serve (ib_insync calls get_event_loop at import).
        try:
            asyncio.get_event_loop().close()
        except Exception:
            pass
        asyncio.set_event_loop(asyncio.new_event_loop())

        # Patch sys.argv so serve.py picks up our port
        argv = ["serve.py", str(port)]
        if tws_port:
            argv += ["--tws-port", str(tws_port)]
        sys.argv = argv

        # Adjust STATIC_DIR if we're running from a bundle
        os.chdir(BASE_DIR)

        # Ensure Cognito env vars are set before importing serve.py
        # (serve reads them at module level)
        _fetch_cognito_from_stack()
        if COGNITO_REGION:
            os.environ.setdefault("COGNITO_REGION", COGNITO_REGION)
        if COGNITO_USER_POOL_ID:
            os.environ.setdefault("COGNITO_USER_POOL_ID", COGNITO_USER_POOL_ID)
        if COGNITO_CLIENT_ID:
            os.environ.setdefault("COGNITO_CLIENT_ID", COGNITO_CLIENT_ID)

        # Import serve — this triggers its global setup
        import importlib

        _log("server_import_serve_start")
        import serve

        importlib.reload(serve)

        serve.STATIC_DIR = Path(BASE_DIR)
        serve.PORT = port

        start_time = time.time()
        try:
            _log("server_main_enter")
            serve.main()
            _log("server_main_exit_clean")
            break  # clean exit
        except SystemExit:
            _log("server_main_system_exit")
            break  # intentional shutdown
        except Exception as exc:
            uptime = time.time() - start_time
            if uptime >= _SERVER_COOLDOWN:
                restarts = 0
            restarts += 1
            print(
                f"[BlitzTrade] serve.py crashed ({exc}), "
                f"restarting in {_SERVER_RESTART_DELAY}s... "
                f"[{restarts}/{_SERVER_MAX_RESTARTS}]"
            )
            _log(
                "server_main_exception: "
                f"{exc} restarts={restarts}/{_SERVER_MAX_RESTARTS} "
                f"uptime={uptime:.1f}s"
            )
            time.sleep(_SERVER_RESTART_DELAY)

    if restarts >= _SERVER_MAX_RESTARTS:
        print(f"[BlitzTrade] Max restarts ({_SERVER_MAX_RESTARTS}) reached — giving up")


# ── Main ─────────────────────────────────────────────────────


def main():
    _install_global_excepthook()
    _log(
        f"launcher_main_start: version={APP_VERSION} "
        f"frozen={getattr(sys, 'frozen', False)} exe={sys.executable} "
        f"onefile_parent={os.environ.get('NUITKA_ONEFILE_PARENT', 'n/a')} "
        f"original_argv0={getattr(globals().get('__compiled__'), 'original_argv0', 'n/a')}"
    )
    if not _ensure_single_instance("BlitzTrade"):
        _log("launcher_exit_single_instance")
        sys.exit(0)

    # Tell Windows this is its own app (not python.exe) so the taskbar
    # shows the BlitzTrade icon instead of the default Python icon.
    if platform.system() == "Windows":
        import ctypes

        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
            "BlitzTrade.BlitzTrade"
        )

    # Fixed port so localStorage persists across restarts (same origin).
    # Falls back to random port if 18710 is already in use.
    port = 18710
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", port))
    except OSError:
        port = _find_free_port()
    _log(f"launcher_port_selected: {port}")
    tws_port = os.environ.get("TWS_PORT")
    tws_port = int(tws_port) if tws_port else None  # None = auto-detect
    _log(f"launcher_tws_port: {tws_port}")

    # Auto-launch IB Gateway only when the saved native startup preference
    # explicitly requests Gateway mode with auto-start enabled.
    if _should_auto_launch_ibgw():
        threading.Thread(target=_auto_launch_ibgw_if_needed, daemon=True).start()
        _log("launcher_auto_ibgw_thread_started")
    else:
        _log("launcher_auto_ibgw_skipped_by_preferences")

    # Check for updates in background
    threading.Thread(target=_check_for_updates, daemon=True).start()
    _log("launcher_update_thread_started")

    # Start aiohttp server in a daemon thread
    server_thread = threading.Thread(
        target=_start_server, args=(port, tws_port), daemon=True
    )
    server_thread.start()
    _log("launcher_server_thread_started")

    # Wait for the server to be ready
    print(f"[BlitzTrade] Waiting for server on port {port}...")
    if not _wait_for_server(port):
        print("[BlitzTrade] Server failed to start within timeout.")
        _log("launcher_exit_server_timeout")
        sys.exit(1)

    print(f"[BlitzTrade] Server ready — opening window")
    _log("launcher_server_ready")

    # Create native window
    api = BlitzAPI()
    # Use bundled icon for taskbar (falls back gracefully if missing)
    icon_path = os.path.join(BASE_DIR, "blitz_trade.ico")
    if platform.system() == "Windows":
        threading.Thread(
            target=_apply_windows_window_icon,
            args=("BlitzTrade", icon_path),
            daemon=True,
        ).start()

    window = webview.create_window(
        "BlitzTrade",
        url=f"http://127.0.0.1:{port}",
        width=1440,
        height=900,
        min_size=(1024, 700),
        maximized=True,
        js_api=api,
        text_select=True,
    )
    _log("launcher_window_created")

    # Persistent storage path for WebView2 (localStorage, cookies, cache).
    # Without this, pywebview may use a temp folder that gets wiped.
    _app_data = os.path.join(
        os.environ.get("LOCALAPPDATA") or os.environ.get("HOME") or BASE_DIR,
        "BlitzTrade",
    )
    os.makedirs(_app_data, exist_ok=True)

    # Start the GUI event loop (blocks until window closes)
    # Force EdgeChromium backend on Windows to avoid fallback to backends
    # that require pythonnet.
    start_kwargs = {
        "debug": ("--debug" in sys.argv),
        "private_mode": False,
        "storage_path": _app_data,
    }
    if platform.system() == "Windows":
        start_kwargs["gui"] = "edgechromium"
        _prepare_windows_pythonnet()
        _prepare_windows_pywebview_shims()

        # Log exact backend import failures to diagnose packaged dependency issues.
        try:
            import importlib

            importlib.import_module("webview.platforms.winforms")
            _log("launcher_winforms_backend_import_ok")
        except Exception:
            _log(
                "launcher_winforms_backend_import_error: "
                + traceback.format_exc().strip()
            )

    try:
        webview.start(**start_kwargs)
    except Exception as e:
        _log(f"launcher_webview_start_error: {e}")
        raise
    _log("launcher_webview_loop_ended")

    print("[BlitzTrade] Window closed — exiting.")
    # Give WebView2 a moment to flush localStorage to disk before killing.
    time.sleep(0.5)
    _log("launcher_force_exit_after_window_close")
    os._exit(0)  # force-kill the server thread


if __name__ == "__main__":
    main()
