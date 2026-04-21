#!/usr/bin/env python3
"""
TWS API bridge for the momentum screener.

Exposes the SAME REST + WebSocket interface that index.html expects,
but uses ib_insync (TWS native API) instead of the Client Portal Gateway.

Key improvement: reqTickByTickData("AllLast") gives every individual trade print
for real T&S, instead of ~250ms aggregated snapshots.

Usage:
    python3 serve.py [port] [--tws-port 7497] [--host 127.0.0.1]

Port defaults:
    TWS paper=7497  TWS live=7496
"""
import asyncio
import ctypes
import json
import logging
import os
import subprocess
import re
import ssl
import sys
import time
import threading
import traceback
import urllib.request
import urllib.error
import http.cookiejar
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

from aiohttp import web

import ib_insync
from ib_insync import (
    IB,
    Stock,
    Contract,
    LimitOrder,
    MarketOrder,
    StopOrder,
    ScannerSubscription,
    TagValue,
    util,
)


_IS_WINDOWS = sys.platform.startswith("win")
_AUTH_STORE_DIR = (
    Path(os.environ.get("LOCALAPPDATA") or os.environ.get("HOME") or str(Path.home()))
    / "BlitzTrade"
)
_AUTH_REFRESH_FILE = _AUTH_STORE_DIR / "auth_refresh_token.bin"


def _dpapi_available():
    return _IS_WINDOWS


def _dpapi_protect(plain: bytes) -> bytes:
    if not _dpapi_available():
        return plain

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [
            ("cbData", ctypes.c_uint32),
            ("pbData", ctypes.POINTER(ctypes.c_char)),
        ]

    crypt32 = ctypes.WinDLL("Crypt32.dll")
    kernel32 = ctypes.WinDLL("Kernel32.dll")

    in_buf = ctypes.create_string_buffer(plain)
    in_blob = DATA_BLOB(len(plain), ctypes.cast(in_buf, ctypes.POINTER(ctypes.c_char)))
    out_blob = DATA_BLOB()

    ok = crypt32.CryptProtectData(
        ctypes.byref(in_blob),
        None,
        None,
        None,
        None,
        0,
        ctypes.byref(out_blob),
    )
    if not ok:
        raise OSError("CryptProtectData failed")

    try:
        return ctypes.string_at(out_blob.pbData, out_blob.cbData)
    finally:
        kernel32.LocalFree(out_blob.pbData)


def _dpapi_unprotect(cipher: bytes) -> bytes:
    if not _dpapi_available():
        return cipher

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [
            ("cbData", ctypes.c_uint32),
            ("pbData", ctypes.POINTER(ctypes.c_char)),
        ]

    crypt32 = ctypes.WinDLL("Crypt32.dll")
    kernel32 = ctypes.WinDLL("Kernel32.dll")

    in_buf = ctypes.create_string_buffer(cipher)
    in_blob = DATA_BLOB(len(cipher), ctypes.cast(in_buf, ctypes.POINTER(ctypes.c_char)))
    out_blob = DATA_BLOB()

    ok = crypt32.CryptUnprotectData(
        ctypes.byref(in_blob),
        None,
        None,
        None,
        None,
        0,
        ctypes.byref(out_blob),
    )
    if not ok:
        raise OSError("CryptUnprotectData failed")

    try:
        return ctypes.string_at(out_blob.pbData, out_blob.cbData)
    finally:
        kernel32.LocalFree(out_blob.pbData)


def _store_refresh_token_secure(token: str) -> bool:
    if not _IS_WINDOWS:
        return False
    try:
        _AUTH_STORE_DIR.mkdir(parents=True, exist_ok=True)
        blob = _dpapi_protect((token or "").encode("utf-8"))
        _AUTH_REFRESH_FILE.write_bytes(blob)
        return True
    except Exception as e:
        log.warning(f"store_refresh_token_secure_failed: {e}")
        return False


def _load_refresh_token_secure() -> str:
    if not _IS_WINDOWS or not _AUTH_REFRESH_FILE.exists():
        return ""
    try:
        blob = _AUTH_REFRESH_FILE.read_bytes()
        plain = _dpapi_unprotect(blob)
        return plain.decode("utf-8", errors="ignore")
    except Exception as e:
        log.warning(f"load_refresh_token_secure_failed: {e}")
        return ""


def _clear_refresh_token_secure() -> None:
    try:
        if _AUTH_REFRESH_FILE.exists():
            _AUTH_REFRESH_FILE.unlink()
    except Exception as e:
        log.warning(f"clear_refresh_token_secure_failed: {e}")


# ── Config ──────────────────────────────────────────────────
PORT = 8888
TWS_HOST = "127.0.0.1"
TWS_PORT = None  # None = auto-detect
CLIENT_ID = 1

# Cognito auth config (stamped at build time, or set via env for dev)
COGNITO_REGION = os.environ.get("COGNITO_REGION", "eu-central-1")
COGNITO_USER_POOL_ID = os.environ.get("COGNITO_USER_POOL_ID", "")
COGNITO_CLIENT_ID = os.environ.get("COGNITO_CLIENT_ID", "")

# Auto-fetch from CloudFormation if not set (dev mode)
if not COGNITO_USER_POOL_ID or not COGNITO_CLIENT_ID:
    try:
        _cfn = subprocess.check_output(
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
        _outs = {o["OutputKey"]: o["OutputValue"] for o in json.loads(_cfn)}
        COGNITO_USER_POOL_ID = COGNITO_USER_POOL_ID or _outs.get("UserPoolId", "")
        COGNITO_CLIENT_ID = COGNITO_CLIENT_ID or _outs.get("DesktopClientId", "")
        if COGNITO_USER_POOL_ID:
            logging.getLogger("tws-bridge").info(
                "Auto-fetched Cognito: pool=%s", COGNITO_USER_POOL_ID
            )
    except Exception:
        pass

# Ports grouped by connection mode preference.
# gateway: IB Gateway live/paper first, then TWS paper/live fallback
# tws: TWS paper/live first, then IB Gateway live/paper fallback
_PORTS_GATEWAY_FIRST = [4001, 4002, 7497, 7496]
_PORTS_TWS_FIRST = [7497, 7496, 4001, 4002]
_connection_mode_preference = "gateway"  # "gateway" or "tws"
_manual_disconnect_in_progress = False


def _candidate_ports_for_mode():
    if _connection_mode_preference == "tws":
        return _PORTS_TWS_FIRST
    return _PORTS_GATEWAY_FIRST


# Parse CLI args
i = 1
while i < len(sys.argv):
    arg = sys.argv[i]
    if arg == "--tws-port" and i + 1 < len(sys.argv):
        TWS_PORT = int(sys.argv[i + 1])
        i += 2
        continue
    elif arg == "--host" and i + 1 < len(sys.argv):
        TWS_HOST = sys.argv[i + 1]
        i += 2
        continue
    elif arg == "--client-id" and i + 1 < len(sys.argv):
        CLIENT_ID = int(sys.argv[i + 1])
        i += 2
        continue
    elif arg == "--cognito-pool" and i + 1 < len(sys.argv):
        COGNITO_USER_POOL_ID = sys.argv[i + 1]
        i += 2
        continue
    elif arg == "--cognito-client" and i + 1 < len(sys.argv):
        COGNITO_CLIENT_ID = sys.argv[i + 1]
        i += 2
        continue
    elif arg.isdigit():
        PORT = int(arg)
    i += 1

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tws-bridge")

# ── Globals ──────────────────────────────────────────────────
ib: IB = None
ib_loop: asyncio.AbstractEventLoop = None
ib_thread: threading.Thread = None
ib_connected = False
ib_account = ""
_boot_ts = int(time.time())  # server boot timestamp — frontend uses to detect restart


# Subscription tracking
class _AutoExpandList(list):
    """List that auto-expands with None on __setitem__ if index is out of range."""

    def __setitem__(self, idx, val):
        while idx >= len(self):
            self.append(None)
        super().__setitem__(idx, val)


_md_subs: dict = {}  # conid -> set of WS clients
_depth_subs: dict = {}  # conid -> set of WS clients
_conid_to_contract: dict = {}
_conid_to_md_ticker: dict = {}  # conid -> Ticker from reqMktData
_conid_to_tbt_ticker: dict = {}  # conid -> Ticker from reqTickByTickData
_conid_to_depth_ticker: dict = {}  # conid -> Ticker from reqMktDepth
_mderr_conids: dict = {}  # conid -> error message (market data not subscribed)
_exec_cache = None  # cached reqExecutionsAsync result
_exec_cache_ts: float = 0  # epoch when _exec_cache was populated
_EXEC_CACHE_TTL: float = 5.0  # seconds to keep executions cache
_ws_clients: set = set()
_http_req_count: int = 0
_watchdog_last_reconnect_mono: float = 0.0
_WD_LOOP_SECS: float = 2.0
_WD_DISCONNECT_RETRY_COOLDOWN_SECS: float = 3.0

# IB enforces a hard cap on simultaneous scanner subscriptions.
# Throttle scanner calls server-side so UI timing spikes cannot exceed the cap.
_SCANNER_MAX_CONCURRENT = 4
_scanner_sem = asyncio.Semaphore(_SCANNER_MAX_CONCURRENT)

# Frozen market data + depth cache for halts
_depth_cache: dict = {}  # conid -> last non-empty depth rows list
_frozen_conids: set = set()  # conids currently using frozen (type 2) market data

# Float cache (from Finviz)
_float_cache: dict = {}
# Country cache: ticker -> 2-letter ISO code (from Finviz)
_country_cache: dict = {}
# Average volume cache: conid -> avg daily volume (30-day)
_avg_vol_cache: dict = {}

# aiohttp event loop (set during startup)
_aio_loop: asyncio.AbstractEventLoop = None


# ══════════════════════════════════════════════════════════════
#  IB CONNECTION (runs in its own thread)
# ══════════════════════════════════════════════════════════════


def _run_ib_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def start_ib():
    global ib, ib_loop, ib_thread
    ib_loop = asyncio.new_event_loop()
    ib = IB()
    ib.RequestTimeout = 30

    # Wire events
    ib.pendingTickersEvent += _on_pending_tickers
    ib.errorEvent += _on_ib_error
    ib.disconnectedEvent += _on_disconnect
    ib.orderStatusEvent += _on_order_status

    # Start loop thread first, then schedule connect
    ib_thread = threading.Thread(target=_run_ib_loop, args=(ib_loop,), daemon=True)
    ib_thread.start()
    ib_loop.call_soon_threadsafe(
        lambda: asyncio.ensure_future(_ib_connect(), loop=ib_loop)
    )


async def _ib_connect():
    global ib_connected, ib_account, TWS_PORT
    ports = [TWS_PORT] if TWS_PORT else _candidate_ports_for_mode()
    for port in ports:
        try:
            await ib.connectAsync(TWS_HOST, port, clientId=CLIENT_ID, readonly=False)
            ib_connected = True
            TWS_PORT = port  # remember which port worked
            accounts = ib.managedAccounts()
            ib_account = accounts[0] if accounts else ""
            log.info(f"Connected to TWS at {TWS_HOST}:{port}  account={ib_account}")
            break
        except ConnectionRefusedError:
            log.info(f"Port {port} not available, trying next...")
            continue
        except Exception as e:
            log.warning(f"Port {port} connect error: {e}")
            continue
    else:
        # None of the ports worked
        ib_connected = False
        tried = ", ".join(str(p) for p in ports)
        log.error(f"TWS connect failed — no response on ports {tried}. Will retry...")
        return
    try:
        # Clear stale ticker caches from previous connection
        _conid_to_md_ticker.clear()
        _conid_to_tbt_ticker.clear()
        _conid_to_depth_ticker.clear()
        # Cancel any pending depth debounce timers
        for h in _depth_pending.values():
            h.cancel()
        _depth_pending.clear()
        _depth_last_real_update.clear()
        _depth_sub_in_flight.clear()
        log.info("Cleared stale ticker caches after reconnect")
        # Re-subscribe market data for all conids with active WS clients
        for conid in list(_md_subs.keys()):
            if _md_subs[conid]:  # has active clients
                try:
                    await _sub_md(conid)
                except Exception as e:
                    log.warning(f"Re-sub MD for {conid} failed: {e}")
        for conid in list(_depth_subs.keys()):
            if _depth_subs[conid]:  # has active clients
                try:
                    await _sub_depth(conid)
                except Exception as e:
                    log.warning(f"Re-sub depth for {conid} failed: {e}")
        # Request account updates so positions/pnl/summary stay fresh
        try:
            ib.reqPnL(ib_account)
        except Exception as e:
            log.warning(f"reqPnL failed (non-fatal): {e}")
        try:
            ib.reqPositions()
        except Exception as e:
            log.warning(f"reqPositions failed (non-fatal): {e}")
        try:
            await ib.reqAccountSummaryAsync()
        except Exception as e:
            log.warning(f"reqAccountSummary failed (non-fatal): {e}")
        try:
            ib.reqAccountUpdates(account=ib_account)
        except Exception as e:
            log.warning(f"reqAccountUpdates failed (non-fatal): {e}")
    except Exception as e:
        log.warning(f"Post-connect setup error (non-fatal): {e}")


def _on_disconnect():
    global ib_connected, TWS_PORT, _manual_disconnect_in_progress
    ib_connected = False
    TWS_PORT = None  # reset so reconnect will auto-detect again
    log.warning("Disconnected from TWS — will try reconnecting in 1s")
    if _manual_disconnect_in_progress:
        _manual_disconnect_in_progress = False
        log.info("Manual reconnect in progress — skipping delayed reconnect schedule")
        return
    try:
        if ib_loop and ib_loop.is_running():
            ib_loop.call_later(
                1, lambda: asyncio.ensure_future(_ib_connect(), loop=ib_loop)
            )
        else:
            log.error("IB event loop not running — cannot schedule reconnect")
    except Exception:
        log.error("Failed to schedule reconnect")


def _on_order_status(trade):
    """Broadcast order status changes to all WS clients in real time."""
    try:
        o, c, st = trade.order, trade.contract, trade.orderStatus
        if getattr(o, "whatIf", False):
            return
        fill_time = ""
        if trade.fills:
            ft = trade.fills[-1].time
            if ft:
                fill_time = (
                    ft.strftime("%y%m%d-%H:%M:%S")
                    if hasattr(ft, "strftime")
                    else str(ft)
                )
        order_time = ""
        if trade.log:
            lt = trade.log[-1].time
            if lt:
                order_time = (
                    lt.strftime("%y%m%d-%H:%M:%S")
                    if hasattr(lt, "strftime")
                    else str(lt)
                )
        avg_price = st.avgFillPrice if st else 0
        commission = 0
        if trade.fills:
            for f in trade.fills:
                cm = f.commissionReport.commission if f.commissionReport else 0
                if cm and cm < 1e8:
                    commission += cm
        rec = {
            "orderId": o.orderId,
            "id": o.orderId,
            "permId": o.permId,
            "conid": c.conId,
            "symbol": c.symbol,
            "ticker": c.symbol,
            "contractDesc": c.symbol,
            "side": o.action,
            "totalSize": o.totalQuantity,
            "filledQuantity": st.filled if st else 0,
            "remainingQuantity": st.remaining if st else 0,
            "orderType": o.orderType,
            "price": o.lmtPrice if o.orderType == "LMT" else 0,
            "auxPrice": o.auxPrice if hasattr(o, "auxPrice") and o.auxPrice else 0,
            "avgPrice": avg_price,
            "status": st.status if st else "Unknown",
            "lastExecutionTime": fill_time,
            "orderTime": order_time,
            "commission": round(commission, 4),
            "realizedPnl": round(_realized_pnl.get(o.orderId, 0), 2),
        }
        msg = json.dumps({"topic": "orderStatus", "order": rec})
        for ws in list(_ws_clients):
            try:
                _send_to_ws(ws, msg)
            except Exception:
                pass
    except Exception as e:
        log.warning(f"_on_order_status error: {e}")


def _ib_watchdog():
    """Background thread: if IB stays disconnected, schedule a reconnect attempt."""
    global _watchdog_last_reconnect_mono
    while True:
        time.sleep(_WD_LOOP_SECS)
        try:
            now = time.monotonic()
            if (
                not ib_connected
                and ib_loop
                and ib_loop.is_running()
                and (now - _watchdog_last_reconnect_mono)
                > _WD_DISCONNECT_RETRY_COOLDOWN_SECS
            ):
                _watchdog_last_reconnect_mono = now
                log.warning("Watchdog: IB still disconnected — scheduling reconnect")
                ib_loop.call_soon_threadsafe(
                    lambda: asyncio.ensure_future(_ib_connect(), loop=ib_loop)
                )
        except Exception as e:
            log.error(f"Watchdog error: {e}")


async def _watchdog_reconnect_ib():
    global _manual_disconnect_in_progress
    try:
        if ib and ib.isConnected():
            _manual_disconnect_in_progress = True
            ib.disconnect()
            await asyncio.sleep(0.25)
            _manual_disconnect_in_progress = False
        await _ib_connect()
    except Exception as e:
        log.warning(f"Watchdog reconnect failed: {e}")
    finally:
        _manual_disconnect_in_progress = False


def _on_ib_error(reqId, errorCode, errorString, contract):
    # Suppress noisy non-errors
    if errorCode in (2104, 2106, 2158, 2119):  # data farm messages
        return
    if (
        errorCode == 162
    ):  # scanner subscription cancelled (normal after reqScannerDataAsync)
        return
    if errorCode == 399:  # order preset informational (e.g. "TIF was set to DAY")
        log.info(f"IB order info {reqId}: {errorString}")
        return
    # Connectivity state changes — log clearly
    if errorCode == 1100:
        log.error("IB connectivity lost — data may be stale")
        return
    if errorCode == 1102:
        log.warning(
            "IB connectivity restored — data loss possible, subscriptions may need refresh"
        )
        return
    if errorCode == 2110:
        log.info("IB connectivity restored")
        return
    # Market data subscription errors — track and broadcast to clients
    if (
        errorCode in (354, 10167, 10090, 10189, 10197)
        and contract
        and getattr(contract, "conId", 0)
    ):
        conid = str(contract.conId)
        _mderr_conids[conid] = errorString
        _broadcast(
            conid,
            _md_subs,
            {"topic": "mderr", "conid": conid, "code": errorCode, "msg": errorString},
        )
        # Depth-specific errors: also notify depth subscribers
        if errorCode in (10090, 10189):
            _broadcast(
                conid,
                _depth_subs,
                {
                    "topic": "deptherr",
                    "conid": conid,
                    "code": errorCode,
                    "msg": errorString,
                },
            )
        log.warning(f"Market data error for conid {conid}: [{errorCode}] {errorString}")
        return
    log.warning(f"IB err {errorCode} reqId={reqId}: {errorString}")
    # Stash order errors so we can return them to the client
    if reqId > 0 and errorCode not in (2104, 2106, 2158, 2119, 399, 354):
        _order_errors[reqId] = f"[{errorCode}] {errorString}"


_order_errors: dict = {}  # orderId -> error string
_realized_pnl: dict = {}  # orderId -> realized P&L (populated by _orders())


def _sched(coro):
    """Schedule a coroutine on the IB loop, return a concurrent Future."""
    return asyncio.run_coroutine_threadsafe(coro, ib_loop)


async def _await_ib(coro, timeout=15):
    """Schedule coro on the IB loop and await it without blocking aiohttp."""
    if not ib_connected:
        raise ConnectionError("IB not connected")
    fut = _sched(coro)
    return await asyncio.wait_for(asyncio.wrap_future(fut), timeout=timeout)


# ── Broadcast helpers ────────────────────────────────────────


def _send_to_ws(ws, data_str):
    """Thread-safe send to a WS client (runs on aiohttp loop)."""
    if _aio_loop and not ws.closed:
        asyncio.run_coroutine_threadsafe(ws.send_str(data_str), _aio_loop)


def _broadcast(conid, subs_dict, msg):
    """Send msg dict to all WS clients subscribed to conid."""
    clients = subs_dict.get(conid)
    if not clients:
        return
    data = json.dumps(msg)
    dead = []
    for ws in list(clients):
        try:
            _send_to_ws(ws, data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


# ══════════════════════════════════════════════════════════════
#  IB EVENT CALLBACKS (run on ib_loop thread)
# ══════════════════════════════════════════════════════════════


def _on_pending_tickers(tickers):
    """Streaming market data updates (from reqMktData)."""
    for t in tickers:
        if not t.contract:
            continue
        conid = str(t.contract.conId)
        # Clear market data error once real data arrives
        if conid in _mderr_conids and _is_num(t.last):
            del _mderr_conids[conid]
        msg = _ticker_to_fields(t, conid)
        if msg:
            _broadcast(conid, _md_subs, msg)


def _ticker_to_fields(t, conid):
    """Convert Ticker -> field-numbered JSON the frontend expects."""
    msg = {"conid": int(conid), "server_id": "tws"}
    rt_trade_cum = None
    rt_trade_size = None
    rt_trade = getattr(t, "rtTradeVolume", None)
    if isinstance(rt_trade, str) and rt_trade:
        # IB rtTradeVolume format: price;size;timestamp;totalVolume;vwap;singleMM
        parts = rt_trade.split(";")
        if len(parts) >= 4:
            try:
                rt_trade_size = float(parts[1])
            except Exception:
                rt_trade_size = None
            try:
                rt_trade_cum = float(parts[3])
            except Exception:
                rt_trade_cum = None
    # Only include last price when it actually changed (avoid stale last on bid/ask updates)
    last_changed = _is_num(t.last) and (not _is_num(t.prevLast) or t.last != t.prevLast)
    if last_changed:
        msg["31"] = str(t.last)
    if _is_num(t.bid):
        msg["84"] = str(t.bid)
    if _is_num(t.ask):
        msg["86"] = str(t.ask)
    if rt_trade_cum is not None and rt_trade_cum >= 0:
        msg["87"] = str(int(rt_trade_cum))
    elif _is_num(t.volume):
        msg["87"] = str(int(t.volume))
    if last_changed:
        if rt_trade_size is not None and rt_trade_size >= 0:
            msg["88"] = str(int(rt_trade_size))
        elif _is_num(t.lastSize):
            msg["88"] = str(int(t.lastSize))
    if last_changed and _is_num(t.close) and t.close > 0:
        chg = t.last - t.close
        msg["82"] = f"{chg:.2f}"
        msg["83"] = f"{chg / t.close * 100:.2f}"
    if _is_num(t.shortableShares):
        if t.shortableShares > 2.5 and conid not in _etb_sent:
            _etb_sent.add(conid)
            msg["_shortable"] = True
    if _is_num(t.halted):
        halted_now = t.halted > 0
        prev = _halt_state.get(conid)
        if halted_now:
            # Going halted: instant — clear any pending unhalt timer
            _halt_unhalt_time.pop(conid, None)
            if prev is not True or conid not in _halt_state:
                _halt_state[conid] = True
                msg["_halted"] = True
                # Switch to Frozen market data (type 2) for last known values
                if conid not in _frozen_conids:
                    _frozen_conids.add(conid)
                    try:
                        ib.reqMarketDataType(2)  # Frozen
                        log.info(f"Switched to Frozen market data for halt: {conid}")
                    except Exception as e:
                        log.warning(f"reqMarketDataType(2) failed for {conid}: {e}")
                # Broadcast cached depth with frozen flag (order book empties during halts)
                _broadcast_cached_depth(conid)
        else:
            # Going unhalted: require 5s of sustained halted=0 (IB oscillates during resume)
            now = time.monotonic()
            if conid not in _halt_unhalt_time:
                _halt_unhalt_time[conid] = now
            if prev is True and (now - _halt_unhalt_time[conid]) >= 5.0:
                _halt_unhalt_time.pop(conid, None)
                _halt_state[conid] = False
                msg["_halted"] = False
                # Switch back to Live market data (type 1)
                if conid in _frozen_conids:
                    _frozen_conids.discard(conid)
                    # Only switch back to Live if no other conids need Frozen
                    if not _frozen_conids:
                        try:
                            ib.reqMarketDataType(1)  # Live
                            log.info("Switched back to Live market data (all unhalted)")
                        except Exception as e:
                            log.warning(f"reqMarketDataType(1) failed: {e}")
            elif prev is not True and conid not in _halt_state:
                # First time seen, not halted — set immediately
                _halt_state[conid] = False
                msg["_halted"] = False
    return msg if len(msg) > 2 else None


_halt_state = {}  # conid -> bool, track per-symbol halt state for change detection
_halt_unhalt_time = (
    {}
)  # conid -> monotonic timestamp of first halted=0 after halted=True
_etb_sent = set()  # conids already confirmed as ETB this session


def _is_num(v):
    return v is not None and v == v  # filters NaN


def _on_tbt_update(ticker, conid):
    """Callback for tick-by-tick AllLast — each call = one real trade print."""
    ticks = ticker.tickByTicks
    if not ticks:
        return
    tick = ticks[-1]
    if not hasattr(tick, "price") or tick.price is None:
        return

    msg = {
        "conid": int(conid),
        "server_id": "tws",
        "31": str(tick.price),
        "88": str(tick.size),
        "_tbt": True,  # flag: this is a real individual trade print
    }
    # Include latest bid/ask from streaming MD
    md = _conid_to_md_ticker.get(conid)
    if md:
        if _is_num(md.bid):
            msg["84"] = str(md.bid)
        if _is_num(md.ask):
            msg["86"] = str(md.ask)
    _broadcast(conid, _md_subs, msg)


_depth_pending: dict = {}  # conid -> asyncio.TimerHandle (debounce)
_depth_seq: dict = {}  # conid -> monotonically increasing depth sequence
_depth_last_real_update: dict = {}  # conid -> time.monotonic() of last TWS callback
_DEPTH_DEBOUNCE = 0.05  # 50 ms — merges rapid per-row TWS callbacks
_DEPTH_STALE_SECS = 30  # re-subscribe if no real update for this long


def _on_depth_update(ticker, conid):
    """Callback for L2 depth updates (debounced)."""
    _depth_last_real_update[conid] = time.monotonic()
    # Cancel any previously scheduled flush for this conid
    h = _depth_pending.pop(conid, None)
    if h:
        h.cancel()
    # Schedule a flush 50 ms from now — if another update arrives before
    # that, this timer is replaced, so only the final book state is sent.
    loop = asyncio.get_event_loop()
    _depth_pending[conid] = loop.call_later(
        _DEPTH_DEBOUNCE, _flush_depth, ticker, conid
    )


def _flush_depth(ticker, conid):
    """Actually broadcast the depth snapshot after debounce."""
    _depth_pending.pop(conid, None)
    clients = _depth_subs.get(conid)
    if not clients:
        return
    seq = _depth_seq.get(conid, 0) + 1
    _depth_seq[conid] = seq
    data = _build_depth_msg(ticker, conid, seq)
    dead = []
    for ws in clients:
        try:
            _send_to_ws(ws, data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


def _build_depth_msg(ticker, conid, seq=None):
    """Build JSON depth message from ticker's domBids/domAsks.

    Sort by price and cap at 20 levels per side to prevent stale entries
    from accumulating in the AutoExpandList when TWS position tracking drifts.
    """
    bids_raw = []
    for level in ticker.domBids or []:
        if level is None or not level.price:
            continue
        sz = int(level.size) if level.size else 0
        if sz <= 0:
            continue
        bids_raw.append(
            (float(level.price), sz, getattr(level, "marketMaker", "") or "")
        )
    asks_raw = []
    for level in ticker.domAsks or []:
        if level is None or not level.price:
            continue
        sz = int(level.size) if level.size else 0
        if sz <= 0:
            continue
        asks_raw.append(
            (float(level.price), sz, getattr(level, "marketMaker", "") or "")
        )
    # Sort: bids descending, asks ascending — then cap at 20 per side
    bids_raw.sort(key=lambda x: -x[0])
    asks_raw.sort(key=lambda x: x[0])
    bids_raw = bids_raw[:20]
    asks_raw = asks_raw[:20]
    # Cross-filter: aggressively remove stale crossed levels.
    # Some feeds briefly deliver mixed old/new levels that can make the
    # top of book crossed and cause UI flicker on ask/bid sides.
    if bids_raw and asks_raw:
        for _ in range(3):
            if not bids_raw or not asks_raw:
                break
            best_bid = bids_raw[0][0]
            best_ask = asks_raw[0][0]
            if best_bid < best_ask:
                break
            new_asks = [a for a in asks_raw if a[0] > best_bid]
            new_bids = [b for b in bids_raw if b[0] < best_ask]
            if len(new_asks) == len(asks_raw) and len(new_bids) == len(bids_raw):
                break
            asks_raw = new_asks
            bids_raw = new_bids
    rows = []
    idx = 0
    for price, sz, mm in bids_raw:
        rows.append(
            {
                "row": idx,
                "focus": 0,
                "price": str(price),
                "bid": sz,
                "ask": "",
                "mm": mm,
            }
        )
        idx += 1
    for price, sz, mm in asks_raw:
        rows.append(
            {
                "row": idx,
                "focus": 0,
                "price": str(price),
                "bid": "",
                "ask": sz,
                "mm": mm,
            }
        )
        idx += 1
    msg = {"topic": f"sbd+{ib_account}+{conid}", "data": rows}
    if seq is not None:
        msg["_seq"] = seq
    # Cache non-empty depth for serving during halts (order book empties on halt)
    if rows:
        _depth_cache[conid] = rows
    return json.dumps(msg)


def _broadcast_cached_depth(conid):
    """Send cached (frozen) depth to all subscribers during a halt."""
    clients = _depth_subs.get(conid)
    if not clients:
        return
    cached = _depth_cache.get(conid)
    if not cached:
        return
    seq = _depth_seq.get(conid, 0) + 1
    _depth_seq[conid] = seq
    msg = {
        "topic": f"sbd+{ib_account}+{conid}",
        "data": cached,
        "_frozen": True,
        "_seq": seq,
    }
    data = json.dumps(msg)
    dead = []
    for ws in clients:
        try:
            _send_to_ws(ws, data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


# ══════════════════════════════════════════════════════════════
#  CONTRACT RESOLUTION + SUBSCRIPTION MANAGEMENT (ib_loop)
# ══════════════════════════════════════════════════════════════


async def _resolve_contract(conid_str):
    if conid_str in _conid_to_contract:
        return _conid_to_contract[conid_str]
    c = Contract(conId=int(conid_str))
    details = await ib.reqContractDetailsAsync(c)
    if details:
        contract = details[0].contract
        _conid_to_contract[conid_str] = contract
        return contract
    return None


async def _sub_md(conid):
    """Subscribe streaming market data + tick-by-tick."""
    if conid in _conid_to_md_ticker:
        return
    contract = await _resolve_contract(conid)
    if not contract:
        log.warning(f"Cannot resolve conid {conid}")
        return
    t = ib.reqMktData(contract, genericTickList="100,236,375", snapshot=False)
    _conid_to_md_ticker[conid] = t
    log.info(f"MD subscribed: {conid} ({contract.symbol})")
    # Also start tick-by-tick for real T&S
    await _sub_tbt(conid, contract)


async def _unsub_md(conid):
    # Re-check: a new client may have subscribed between the schedule and execution
    clients = _md_subs.get(conid)
    if clients:
        return  # still has active listeners — keep the subscription
    t = _conid_to_md_ticker.pop(conid, None)
    if not t:
        return
    ib.cancelMktData(t.contract)
    await _unsub_tbt(conid)
    log.info(f"MD unsubscribed: {conid}")


async def _sub_tbt(conid, contract=None):
    if conid in _conid_to_tbt_ticker:
        return
    if not contract:
        contract = await _resolve_contract(conid)
    if not contract:
        return
    t = ib.reqTickByTickData(contract, tickType="AllLast")
    _conid_to_tbt_ticker[conid] = t
    t.updateEvent += lambda ticker, c=conid: _on_tbt_update(ticker, c)
    log.info(f"TBT subscribed: {conid} ({contract.symbol})")


async def _unsub_tbt(conid):
    t = _conid_to_tbt_ticker.pop(conid, None)
    if t and t.contract:
        ib.cancelTickByTickData(t.contract, "AllLast")


_depth_sub_in_flight: set = set()  # conids currently being subscribed


async def _sub_depth(conid, force=False):
    if conid in _depth_sub_in_flight:
        return  # already subscribing — skip duplicate
    if not force and conid in _conid_to_depth_ticker:
        return  # already active
    _depth_sub_in_flight.add(conid)
    try:
        # On forced refresh, cancel existing and re-subscribe to get a fresh book.
        old = _conid_to_depth_ticker.pop(conid, None)
        if old and old.contract:
            try:
                ib.cancelMktDepth(old.contract)
            except Exception:
                pass
        contract = await _resolve_contract(conid)
        if not contract:
            return
        t = ib.reqMktDepth(contract, numRows=20, isSmartDepth=True)
        # Replace domBids/domAsks with auto-expanding lists to prevent index errors
        t.domBids = _AutoExpandList(t.domBids)
        t.domAsks = _AutoExpandList(t.domAsks)
        _conid_to_depth_ticker[conid] = t

        def _depth_cb(ticker, c=conid):
            try:
                _on_depth_update(ticker, c)
            except Exception as e:
                log.error(f"Depth callback error for {c}: {e}")

        t.updateEvent += _depth_cb
        _depth_last_real_update[conid] = time.monotonic()
        log.info(f"Depth subscribed: {conid} force={force}")
    finally:
        _depth_sub_in_flight.discard(conid)


async def _unsub_depth(conid):
    # Don't unsub if a new subscription is in-flight (it would cancel the new one)
    if conid in _depth_sub_in_flight:
        return
    # Re-check: a new client may have subscribed between the schedule and execution
    clients = _depth_subs.get(conid)
    if clients:
        return  # still has active listeners — keep the subscription
    h = _depth_pending.pop(conid, None)
    if h:
        h.cancel()
    _depth_last_real_update.pop(conid, None)
    t = _conid_to_depth_ticker.pop(conid, None)
    if not t:
        return
    if t.contract:
        ib.cancelMktDepth(t.contract)
    log.info(f"Depth unsubscribed: {conid}")


# ══════════════════════════════════════════════════════════════
#  REST HANDLERS
# ══════════════════════════════════════════════════════════════


async def h_auth_status(req):
    return web.json_response(
        {
            "authenticated": ib_connected,
            "connected": ib_connected,
            "competing": False,
            "fail": "",
            "message": "",
            "prompts": [],
            "boot": _boot_ts,
            "twsPort": TWS_PORT,
        }
    )


async def h_health(req):
    """Lightweight health endpoint polled by the external bridge watchdog."""
    return web.json_response(
        {
            "ok": True,
            "boot": _boot_ts,
            "uptime": round(time.time() - _boot_ts, 1),
            "ib_connected": ib_connected,
            "ws_clients": len(_ws_clients),
            "md_subs": len(_md_subs),
            "depth_subs": len(_depth_subs),
        }
    )


def force_restart():
    """Stop the aiohttp event loop — launcher's _start_server loop will restart the bridge."""
    global _aio_loop
    log.warning("force_restart() called — stopping aio loop for clean restart")
    if _aio_loop and _aio_loop.is_running():
        _aio_loop.call_soon_threadsafe(_aio_loop.stop)


async def h_scanner_run(req):
    body = await req.json()
    scan_code = body.get("sortBy", body.get("type", "MOST_ACTIVE"))
    above_price = body.get("abovePrice", 0.5)
    below_price = body.get("belowPrice", 0)
    above_vol = body.get("aboveVolume", 0)
    mkt_cap_above = body.get("marketCapAbove", 0)
    mkt_cap_below = body.get("marketCapBelow", 0)
    num_rows = min(int(body.get("numberOfRows", 25)), 50)
    location = body.get("location", "STK.US")
    sub = ScannerSubscription(
        instrument="STK",
        locationCode=location,
        scanCode=scan_code,
        abovePrice=above_price,
        numberOfRows=num_rows,
    )
    if below_price > 0:
        sub.belowPrice = below_price
    if above_vol > 0:
        sub.aboveVolume = int(above_vol)
    if mkt_cap_above > 0:
        sub.marketCapAbove = mkt_cap_above
    if mkt_cap_below > 0:
        sub.marketCapBelow = mkt_cap_below
    # Build scannerSettingPairs for change% filters
    pairs = []
    chg_above = body.get("changePercAbove", 0)
    chg_below = body.get("changePercBelow", 0)
    if chg_above > 0:
        pairs.append(f"changePercAbove={chg_above}")
    if chg_below > 0:
        pairs.append(f"changePercBelow={chg_below}")
    if pairs:
        sub.scannerSettingPairs = ";".join(pairs)
    try:
        result = await _await_ib(_scanner(sub), timeout=15)
    except Exception as e:
        log.error(f"Scanner: {e}")
        return web.json_response({"contracts": []})
    return web.json_response({"contracts": result})


async def _scanner(sub):
    try:
        async with _scanner_sem:
            data = await ib.reqScannerDataAsync(sub)
        out = []
        for sd in data or []:
            c = sd.contractDetails.contract
            _conid_to_contract[str(c.conId)] = c
            out.append(
                {
                    "con_id": c.conId,
                    "conid": c.conId,
                    "symbol": c.symbol,
                    "company_header": f"{c.symbol} - {sd.contractDetails.longName}",
                    "companyHeader": f"{c.symbol} - {sd.contractDetails.longName}",
                    "company_name": sd.contractDetails.longName,
                    "sec_type": c.secType,
                    "exchange": c.primaryExchange or "",
                }
            )
        return out
    except Exception as e:
        log.error(f"Scanner error: {e}")
        return []


async def h_depth_snapshot(req):
    """REST endpoint: return current L2 book snapshot for a conid."""
    conid = req.query.get("conid", "").strip()
    if not conid:
        return web.json_response([])
    t = _conid_to_depth_ticker.get(conid)
    rows = []
    if t:
        # Reuse _build_depth_msg logic (sort, cap, cross-filter)
        msg = json.loads(_build_depth_msg(t, conid))
        rows = msg.get("data", [])
    # During halts or if live book is empty, fall back to cached depth
    if not rows and conid in _depth_cache:
        rows = _depth_cache[conid]
    return web.json_response(rows)


async def h_depth_probe(req):
    """Debug endpoint: probe SMART vs direct depth for a conid."""
    conid = req.query.get("conid", "").strip()
    if not conid:
        return web.json_response({"error": "conid required"}, status=400)
    try:
        result = await _await_ib(_depth_probe(conid), timeout=20)
    except Exception as e:
        log.error(f"Depth probe: {e}")
        return web.json_response({"error": str(e)}, status=500)
    return web.json_response(result)


async def _depth_probe(conid):
    contract = await _resolve_contract(conid)
    if not contract:
        return {"conid": conid, "error": "contract not found"}

    out = {
        "conid": conid,
        "symbol": getattr(contract, "symbol", ""),
        "exchange": getattr(contract, "exchange", ""),
        "primaryExchange": getattr(contract, "primaryExchange", ""),
        "modes": [],
        "mdError": _mderr_conids.get(conid),
    }

    for is_smart in (True, False):
        rec = {
            "mode": "smart" if is_smart else "direct",
            "ok": False,
            "rows": 0,
            "bids": 0,
            "asks": 0,
            "error": None,
        }
        t = None
        try:
            t = ib.reqMktDepth(contract, numRows=20, isSmartDepth=is_smart)
            t.domBids = _AutoExpandList(t.domBids)
            t.domAsks = _AutoExpandList(t.domAsks)
            await asyncio.sleep(1.5)
            msg = json.loads(_build_depth_msg(t, conid))
            rows = msg.get("data", [])
            rec["rows"] = len(rows)
            rec["bids"] = len([r for r in rows if r.get("bid") not in ("", None)])
            rec["asks"] = len([r for r in rows if r.get("ask") not in ("", None)])
            rec["ok"] = True
        except Exception as e:
            rec["error"] = str(e)
        finally:
            if t and t.contract:
                try:
                    ib.cancelMktDepth(t.contract)
                except Exception:
                    pass
        out["modes"].append(rec)

    return out


async def h_snapshot(req):
    cids = [c.strip() for c in req.query.get("conids", "").split(",") if c.strip()]
    if not cids:
        return web.json_response([])
    try:
        result = await _await_ib(_snapshots(cids), timeout=15)
    except Exception as e:
        log.error(f"Snapshot: {e}")
        return web.json_response([])
    return web.json_response(result)


async def _snapshots(cids):
    """Get snapshot market data for a list of conids.

    Uses reqTickersAsync for one-shot snapshots.  Prefers last trade
    price to match chart candles; falls back to bid/ask midpoint, then close.
    """
    contracts = []
    for cid in cids:
        c = await _resolve_contract(cid)
        if c:
            contracts.append(c)
    if not contracts:
        return []
    tickers = await ib.reqTickersAsync(*contracts)
    out = []
    for t in tickers:
        if not t.contract:
            continue
        r = {
            "conid": t.contract.conId,
            "conidEx": str(t.contract.conId),
            "55": t.contract.symbol,
            "7051": t.contract.symbol,
        }
        # Prefer last trade price (matches chart candles); fall back to midpoint,
        # then close.  On weekends/after-hours last & midpoint may be NaN, so
        # also try the previous close as the display price.
        price = t.last if _is_num(t.last) else None
        if price is None:
            mp = t.marketPrice()
            price = mp if _is_num(mp) else None
        if price is None and _is_num(t.close) and t.close > 0:
            price = t.close
        if price is not None:
            r["31"] = f"{price:.4f}"
        if _is_num(t.bid):
            r["84"] = str(t.bid)
        if _is_num(t.ask):
            r["86"] = str(t.ask)
        if _is_num(t.volume):
            r["87"] = str(int(t.volume))
        if _is_num(t.lastSize):
            r["88"] = str(int(t.lastSize))
        if _is_num(t.open):
            r["7295"] = str(t.open)
        if _is_num(t.high):
            r["7293"] = str(t.high)
        if _is_num(t.low):
            r["7294"] = str(t.low)
        if _is_num(t.avVolume):
            r["7282"] = str(int(t.avVolume))
        else:
            # avVolume is NaN from reqTickersAsync — use cached historical avg
            cid_str = str(t.contract.conId)
            if cid_str in _avg_vol_cache:
                r["7282"] = str(_avg_vol_cache[cid_str])
        if _is_num(t.close) and price is not None and t.close > 0:
            chg = price - t.close
            r["70"] = str(t.close)
            r["82"] = f"{chg:.2f}"
            r["83"] = f"{chg / t.close * 100:.2f}"
        if _is_num(t.halted):
            r["_halted"] = t.halted > 0
        if _is_num(t.shortableShares) and t.shortableShares > 2.5:
            r["_shortable"] = True
        out.append(r)
    # Background-fill avg volume cache for any conids that don't have it yet
    missing = [
        str(c.conId)
        for c in contracts
        if str(c.conId) not in _avg_vol_cache
        and not any(
            _is_num(t.avVolume)
            for t in tickers
            if t.contract and t.contract.conId == c.conId
        )
    ]
    if missing:
        fut = _sched(_fill_avg_vol(missing))
        fut.add_done_callback(
            lambda f: (
                log.warning(f"_fill_avg_vol error: {f.exception()}")
                if not f.cancelled() and f.exception()
                else None
            )
        )
    return out


async def _fill_avg_vol(cids):
    """Compute 30-day average daily volume from historical bars and cache it."""

    async def _fetch_one(cid):
        if cid in _avg_vol_cache:
            return
        try:
            contract = await _resolve_contract(cid)
            if not contract:
                return
            bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime="",
                durationStr="30 D",
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=True,
                formatDate=2,
            )
            if bars:
                vols = [b.volume for b in bars if b.volume > 0]
                if vols:
                    avg = int(sum(vols) / len(vols))
                    _avg_vol_cache[cid] = avg
                    log.info(f"AvgVol cached: {contract.symbol} = {avg:,}")
        except Exception as e:
            log.warning(f"AvgVol failed for {cid}: {e}")

    # Fetch in parallel batches of 5 to avoid overwhelming TWS
    batch_size = 5
    for i in range(0, len(cids), batch_size):
        batch = cids[i : i + batch_size]
        await asyncio.gather(*[_fetch_one(c) for c in batch])


# ── History cache (conid:period:bar:endDt → {data, ts}) ──
_history_cache: dict = {}
_HISTORY_TTL = 30  # seconds
_HISTORY_MAX_ENTRIES = 500  # max cache entries before eviction


def _evict_history_cache():
    """Remove expired entries; if still over limit, drop oldest."""
    if not _history_cache:
        return
    now = asyncio.get_event_loop().time()
    expired = [
        k for k, v in _history_cache.items() if (now - v["ts"]) > _HISTORY_TTL * 10
    ]
    for k in expired:
        del _history_cache[k]
    if len(_history_cache) > _HISTORY_MAX_ENTRIES:
        # Drop oldest half
        by_age = sorted(_history_cache.items(), key=lambda x: x[1]["ts"])
        for k, _ in by_age[: len(by_age) // 2]:
            del _history_cache[k]


async def h_history(req):
    conid = req.query.get("conid", "")
    period = req.query.get("period", "1d")
    bar = req.query.get("bar", "1min")
    dur_map = {
        "30S": "30 S",
        "60S": "60 S",
        "120S": "120 S",
        "300S": "300 S",
        "600S": "600 S",
        "1800S": "1800 S",
        "3600S": "3600 S",
        "1d": "1 D",
        "2d": "2 D",
        "1w": "1 W",
        "2w": "2 W",
        "1m": "1 M",
        "3m": "3 M",
        "6m": "6 M",
        "1y": "1 Y",
    }
    bar_map = {
        "1secs": "1 secs",
        "5secs": "5 secs",
        "10secs": "10 secs",
        "15secs": "15 secs",
        "30secs": "30 secs",
        "1min": "1 min",
        "2mins": "2 mins",
        "3mins": "3 mins",
        "5mins": "5 mins",
        "10mins": "10 mins",
        "15mins": "15 mins",
        "20mins": "20 mins",
        "30mins": "30 mins",
        "1hour": "1 hour",
        "2hours": "2 hours",
        "4hours": "4 hours",
        "1day": "1 day",
        "1week": "1 week",
        "1month": "1 month",
    }
    try:
        end_dt = ""
        end_dt_str = req.query.get("endDateTime", "")
        if end_dt_str:
            end_dt = datetime.fromtimestamp(int(end_dt_str), tz=timezone.utc)
        duration = dur_map.get(period, "1 D")
        bar_size = bar_map.get(bar, "1 min")
        # Check cache (skip for live edge — no endDateTime)
        cache_key = f"{conid}:{period}:{bar}:{end_dt_str}"
        now = asyncio.get_event_loop().time()
        cached = _history_cache.get(cache_key)
        if cached and (now - cached["ts"]) < _HISTORY_TTL:
            return web.json_response(cached["data"])
        # Keep below frontend fetch timeout (20s), then gracefully degrade to MIDPOINT.
        result = await _await_ib(
            _history_with_retries(conid, duration, bar_size, end_dt),
            timeout=22,
        )
        if result and result.get("data"):
            _history_cache[cache_key] = {"data": result, "ts": now}
            if len(_history_cache) > _HISTORY_MAX_ENTRIES:
                _evict_history_cache()
    except Exception as e:
        log.error(f"History: {e}")
        return web.json_response({"data": []})
    return web.json_response(result)


async def _history(conid, duration, bar_size, end_dt="", what_to_show="TRADES"):
    contract = await _resolve_contract(conid)
    if not contract:
        return {"data": []}
    try:
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime=end_dt,
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow=what_to_show,
            useRTH=False,
            formatDate=2,
        )
        data = []
        for b in bars or []:
            if isinstance(b.date, datetime):
                ts = int(b.date.timestamp() * 1000)
            else:
                ts = int(float(str(b.date)) * 1000)
            data.append(
                {
                    "t": ts,
                    "o": b.open,
                    "h": b.high,
                    "l": b.low,
                    "c": b.close,
                    "v": int(b.volume),
                }
            )
        return {"data": data}
    except Exception as e:
        log.error(f"History fetch: {e}")
        return {"data": []}


def _history_retry_durations(duration):
    """Return progressively wider duration windows for real-data retries."""
    d = str(duration or "").strip().upper()
    ladder = [d]
    # Keep retries conservative and only widen when needed.
    if d in {"30 S", "60 S", "120 S", "300 S", "600 S", "1800 S", "3600 S"}:
        ladder.extend(["2 D", "5 D"])
    elif d in {"1 D", "2 D"}:
        ladder.extend(["5 D", "1 M"])
    elif d == "1 W":
        ladder.extend(["2 W", "1 M"])
    elif d == "2 W":
        ladder.extend(["1 M", "3 M"])
    elif d == "1 M":
        ladder.append("3 M")
    # De-duplicate preserving order
    out = []
    for x in ladder:
        if x and x not in out:
            out.append(x)
    return out


async def _history_with_retries(conid, duration, bar_size, end_dt=""):
    """Fetch real IB history with TRADES->MIDPOINT and wider duration retries."""
    durations = _history_retry_durations(duration)
    for dur in durations:
        trades = await _history(
            conid,
            dur,
            bar_size,
            end_dt,
            what_to_show="TRADES",
        )
        if trades and trades.get("data"):
            return trades

        midpoint = await _history(
            conid,
            dur,
            bar_size,
            end_dt,
            what_to_show="MIDPOINT",
        )
        if midpoint and midpoint.get("data"):
            return midpoint

    log.warning(
        f"History empty after retries: conid={conid} duration={duration} bar={bar_size}"
    )
    return {"data": []}


# ── User Settings (persistent server-side backup) ─────────────
_SETTINGS_FILE = Path(__file__).parent / "settings.json"
_RELEASE_NOTES_DIR = Path(__file__).parent / "release_notes"
_RELEASE_NOTES_FILE_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)\.json$", re.IGNORECASE)


async def h_settings_get(request):
    if _SETTINGS_FILE.exists():
        try:
            return web.json_response(json.loads(_SETTINGS_FILE.read_text()))
        except Exception:
            return web.json_response({})
    return web.json_response({})


async def h_settings_save(request):
    try:
        data = await request.json()
        _SETTINGS_FILE.write_text(json.dumps(data, indent=1))
        return web.json_response({"ok": True})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=400)


def _release_notes_version(name: str):
    m = _RELEASE_NOTES_FILE_RE.match(name)
    if not m:
        return None
    return tuple(int(part) for part in m.groups())


def _load_latest_release_notes():
    latest_path = _RELEASE_NOTES_DIR / "latest.json"
    if latest_path.exists():
        data = json.loads(latest_path.read_text())
        version = str(data.get("version", "")).strip()
        return {
            "version": version,
            "summary": str(data.get("summary", "")).strip(),
            "release_notes": data.get("release_notes") or [],
        }
    if not _RELEASE_NOTES_DIR.exists():
        return None
    versioned = []
    for path in _RELEASE_NOTES_DIR.glob("*.json"):
        version = _release_notes_version(path.name)
        if version is None:
            continue
        versioned.append((version, path))
    if not versioned:
        return None
    version, path = max(versioned, key=lambda item: item[0])
    data = json.loads(path.read_text())
    return {
        "version": ".".join(str(part) for part in version),
        "summary": str(data.get("summary", "")).strip(),
        "release_notes": data.get("release_notes") or [],
    }


async def h_release_notes_latest(request):
    try:
        data = _load_latest_release_notes()
    except Exception as e:
        log.error(f"Failed to load release notes: {e}")
        return web.json_response({"error": "Failed to load release notes"}, status=500)
    if not data:
        return web.json_response({"version": "", "summary": "", "release_notes": []})
    return web.json_response(data)


async def h_release_notes_all(request):
    """Return all release notes sorted newest-first."""
    try:
        if not _RELEASE_NOTES_DIR.exists():
            return web.json_response([])
        results = []
        for path in _RELEASE_NOTES_DIR.glob("*.json"):
            ver = _release_notes_version(path.name)
            if ver is None:
                continue
            data = json.loads(path.read_text())
            results.append(
                {
                    "version": ".".join(str(p) for p in ver),
                    "_sort": ver,
                    "summary": str(data.get("summary", "")).strip(),
                    "release_notes": data.get("release_notes") or [],
                }
            )
        results.sort(key=lambda r: r["_sort"], reverse=True)
        results = results[:3]
        for r in results:
            del r["_sort"]
        return web.json_response(results)
    except Exception as e:
        log.error(f"Failed to load release notes: {e}")
        return web.json_response([], status=500)


# ── Trade Journal (persistent trade log) ──────────────────────
_TRADES_FILE = Path(__file__).parent / "trades.json"
_trade_journal: list = []  # list of trade records
_seen_trade_keys: set = set()  # permId or orderId keys to avoid dupes


def _load_journal():
    global _trade_journal, _seen_trade_keys
    if _TRADES_FILE.exists():
        try:
            _trade_journal = json.loads(_TRADES_FILE.read_text())
            _seen_trade_keys = {r.get("key", "") for r in _trade_journal}
        except Exception as e:
            log.error(f"Failed to load trades.json: {e}")


def _save_journal():
    try:
        _TRADES_FILE.write_text(json.dumps(_trade_journal, indent=1))
    except Exception as e:
        log.error(f"Failed to save trades.json: {e}")


def _journal_add_trade(rec):
    """Add a filled trade record if not already present."""
    key = rec.get("key", "")
    if key in _seen_trade_keys:
        return False
    _seen_trade_keys.add(key)
    _trade_journal.append(rec)
    _save_journal()
    return True


async def h_trades_journal(req):
    """GET /api/trades — return full journal."""
    return web.json_response({"trades": _trade_journal})


async def h_trades_clear(req):
    """DELETE /api/trades — clear all trades."""
    global _trade_journal, _seen_trade_keys
    _trade_journal = []
    _seen_trade_keys = set()
    _save_journal()
    return web.json_response({"cleared": True})


async def h_trades_import_csv(req):
    """POST /api/trades/import — import trades from IB Activity Statement CSV or JSON."""
    global _trade_journal, _seen_trade_keys
    # Clear existing trades — import always replaces (not appends)
    _trade_journal = []
    _seen_trade_keys = set()

    try:
        reader = await req.multipart()
        field = await reader.next()
        raw = await field.read(decode=False)
        text = raw.decode("utf-8-sig")  # handle BOM
    except Exception as e:
        # Fallback: try plain body
        try:
            raw = await req.read()
            text = raw.decode("utf-8-sig")
        except Exception as e2:
            return web.json_response(
                {"error": f"Failed to read upload: {e2}"}, status=400
            )

    text = text.strip()

    # ── JSON format ──
    if text.startswith("[") or text.startswith("{"):
        return _import_json_trades(text)

    # ── CSV format ──
    return _import_csv_trades(text)


def _import_json_trades(text):
    """Import trades from a JSON array or object with trades array."""
    added = 0
    errors = []
    try:
        data = json.loads(text)
    except Exception as e:
        return web.json_response({"error": f"Invalid JSON: {e}"}, status=400)

    records = (
        data
        if isinstance(data, list)
        else data.get("trades", data.get("orders", data.get("data", [])))
    )
    if not isinstance(records, list) or not records:
        return web.json_response(
            {
                "error": "JSON must be an array of trade objects or contain a trades/orders/data array.",
                "added": 0,
            }
        )

    # Auto-detect field names from first record
    sample = records[0]
    keys = {k.lower().replace(" ", "").replace("_", ""): k for k in sample.keys()}

    def find_key(*names):
        for n in names:
            nl = n.lower().replace(" ", "").replace("_", "")
            for norm, orig in keys.items():
                if nl in norm or norm in nl:
                    return orig
        return None

    k_sym = find_key("symbol", "tradeable", "ticker", "instrument")
    k_dt = find_key(
        "datetime", "timestamp", "time", "date", "executiontimestamp", "filledtime"
    )
    k_qty = find_key("quantity", "qty", "size", "amount", "executionquantity")
    k_side = find_key("direction", "side", "action", "buysell", "orderside")
    k_price = find_key(
        "price", "avgprice", "executionprice", "limitprice", "tprice", "fillprice"
    )
    k_comm = find_key("commission", "comm", "fee", "commfee")
    k_pnl = find_key("realizedpnl", "realizedpl", "pnl", "profit")
    k_status = find_key("status", "eventtype", "state")

    if not k_sym:
        return web.json_response(
            {
                "error": f"Cannot find symbol field. Keys found: {list(sample.keys())[:15]}",
                "added": 0,
            }
        )

    for rec in records:
        try:
            sym = str(rec.get(k_sym, "")).strip()
            if not sym:
                continue
            # Clean symbol (e.g. "PF_SOLUSD" -> "SOLUSD")
            if "_" in sym:
                sym = sym.split("_", 1)[-1]

            # Parse timestamp
            dt_raw = rec.get(k_dt, "") if k_dt else ""
            if isinstance(dt_raw, (int, float)) and dt_raw > 1e12:
                dt = datetime.fromtimestamp(dt_raw / 1000)  # millis
            elif isinstance(dt_raw, (int, float)):
                dt = datetime.fromtimestamp(dt_raw)
            elif dt_raw:
                dt_str = str(dt_raw).replace(", ", " ").replace("T", " ").strip()
                dt = None
                for fmt in (
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M:%S.%f",
                    "%Y%m%d-%H:%M:%S",
                    "%Y%m%d",
                    "%m/%d/%Y %H:%M:%S",
                    "%Y-%m-%d",
                ):
                    try:
                        dt = datetime.strptime(dt_str[:19], fmt)
                        break
                    except ValueError:
                        continue
                if not dt:
                    errors.append(f"Bad date: {dt_raw}")
                    continue
            else:
                continue

            # Quantity & side
            qty_raw = rec.get(k_qty, 0) if k_qty else 0
            qty = abs(float(qty_raw)) if qty_raw else 0

            side_raw = str(rec.get(k_side, "")).upper() if k_side else ""
            if side_raw in ("BUY", "BOT", "B", "LONG"):
                side = "BUY"
            elif side_raw in ("SELL", "SLD", "S", "SHORT"):
                side = "SELL"
            elif qty_raw and float(qty_raw) < 0:
                side = "SELL"
                qty = abs(float(qty_raw))
            else:
                side = "BUY"

            price = float(rec.get(k_price, 0) or 0) if k_price else 0
            comm = abs(float(rec.get(k_comm, 0) or 0)) if k_comm else 0
            pnl = float(rec.get(k_pnl, 0) or 0) if k_pnl else 0

            # Optional status filter — skip cancelled/rejected
            if k_status:
                st = str(rec.get(k_status, "")).lower()
                if st in ("cancelled", "canceled", "rejected", "expired", "inactive"):
                    continue

            fill_time = dt.strftime("%y%m%d-%H:%M:%S")
            key = f"json_{sym}_{fill_time}_{qty}_{price}"

            trade = {
                "key": key,
                "orderId": 0,
                "permId": 0,
                "symbol": sym,
                "conid": 0,
                "side": side,
                "qty": qty,
                "avgPrice": round(price, 4),
                "commission": round(comm, 4),
                "realizedPnl": round(pnl, 6),
                "fillTime": fill_time,
                "orderType": "JSON",
                "status": "Filled",
            }
            if _journal_add_trade(trade):
                added += 1
        except Exception as ex:
            errors.append(str(ex))
            if len(errors) > 10:
                break

    return web.json_response(
        {
            "added": added,
            "total": len(_trade_journal),
            "errors": errors[:5],
            "parsed_rows": len(records),
        }
    )


def _import_csv_trades(text):
    added = 0
    errors = []

    # Detect format: IB Activity Statement has sections starting with "Trades,"
    # or it could be a simple CSV with headers
    trade_lines = []
    in_trades = False
    header_row = None
    account_name = None

    # Use proper CSV parsing to handle quoted fields with commas
    import csv, io

    all_rows = list(csv.reader(io.StringIO(text)))

    # Track the first (stocks) header; skip Forex and subsequent header overrides
    stock_header_row = None

    for row in all_rows:
        if not row:
            continue
        # Extract account name from IB Activity Statement
        if (
            len(row) >= 4
            and row[0].strip() == "Account Information"
            and row[1].strip() == "Data"
            and row[2].strip() == "Name"
        ):
            account_name = row[3].strip()
        # IB Activity Statement format: first fields are section,type
        if len(row) >= 3 and row[0].strip() == "Trades" and row[1].strip() == "Header":
            cols = [c.strip() for c in row[2:]]
            if stock_header_row is None:
                # Keep the first header (Stocks) which has Realized P/L etc.
                stock_header_row = cols
                header_row = cols
            in_trades = True
            continue
        if in_trades:
            if (
                len(row) >= 3
                and row[0].strip() == "Trades"
                and row[1].strip() == "Data"
            ):
                fields = [c.strip() for c in row[2:]]
                # Skip subtotal/total rows
                if any("SubTotal" in c or "Total" in c for c in row):
                    continue
                # Skip non-stock asset categories (Forex, etc.)
                # In IB format, DataDiscriminator is field[0], Asset Category is field[1]
                if len(fields) > 1 and fields[1] in (
                    "Forex",
                    "Options",
                    "Futures",
                    "Bonds",
                ):
                    continue
                trade_lines.append(fields)
            elif len(row) >= 1 and row[0].strip() == "Trades":
                continue  # skip other Trades rows (subtotals etc)
            else:
                in_trades = False

    # If no IB Activity Statement format detected, try plain CSV
    if not trade_lines and all_rows:
        # Use all_rows directly — already parsed
        header_row = [h.strip() for h in all_rows[0]]
        trade_lines = [r for r in all_rows[1:] if len(r) >= len(header_row) // 2]

    log.info(
        f"CSV import: header={header_row[:8] if header_row else None}, trade_lines={len(trade_lines)}"
    )

    if not header_row or not trade_lines:
        return web.json_response(
            {
                "error": "Could not parse CSV. Expected IB Activity Statement or CSV with headers: Symbol, Date/Time, Quantity, T. Price, Comm/Fee, Realized P&L",
                "added": 0,
            }
        )

    # Normalize header names
    hdr = [
        h.strip().lower().replace(" ", "").replace(".", "").replace("/", "")
        for h in header_row
    ]

    # Map common IB column names
    def find_col(*names):
        for n in names:
            nl = n.lower().replace(" ", "").replace(".", "").replace("/", "")
            for i, h in enumerate(hdr):
                if nl in h or h in nl:
                    return i
        return -1

    i_sym = find_col("symbol")
    i_dt = find_col("datetime", "date/time", "tradetime", "date")
    i_qty = find_col("quantity", "qty")
    i_price = find_col("tprice", "tradeprice", "price", "avgprice")
    i_comm = find_col("commfee", "commission", "comm")
    i_pnl = find_col("realizedpl", "realizedpnl", "realized", "pnl")
    i_code = find_col("code", "codes")

    col_map = {
        "sym": i_sym,
        "dt": i_dt,
        "qty": i_qty,
        "price": i_price,
        "comm": i_comm,
        "pnl": i_pnl,
    }
    sample_row = trade_lines[0] if trade_lines else []

    if i_sym < 0 or i_dt < 0:
        return web.json_response(
            {
                "error": f"Missing required columns. Found: {header_row[:10]}. Need at least Symbol and Date/Time.",
                "added": 0,
                "col_map": col_map,
            }
        )

    skipped = 0
    for row in trade_lines:
        try:
            if len(row) <= max(i_sym, i_dt):
                skipped += 1
                continue
            sym = row[i_sym].strip().strip('"')
            dt_str = row[i_dt].strip().strip('"')
            if not sym or not dt_str:
                skipped += 1
                continue

            qty_raw = (
                row[i_qty].strip().strip('"').replace(",", "")
                if i_qty >= 0 and i_qty < len(row)
                else "0"
            )
            if not qty_raw or qty_raw == "--":
                skipped += 1
                continue
            try:
                qty = float(qty_raw)
            except ValueError:
                skipped += 1
                continue
            side = "BUY" if qty > 0 else "SELL"
            qty = abs(qty)

            price = (
                float(row[i_price].strip().strip('"').replace(",", ""))
                if i_price >= 0 and i_price < len(row) and row[i_price].strip()
                else 0
            )
            comm = (
                float(row[i_comm].strip().strip('"').replace(",", ""))
                if i_comm >= 0 and i_comm < len(row) and row[i_comm].strip()
                else 0
            )
            pnl = (
                float(row[i_pnl].strip().strip('"').replace(",", ""))
                if i_pnl >= 0 and i_pnl < len(row) and row[i_pnl].strip()
                else 0
            )

            # Parse date: IB uses "YYYY-MM-DD, HH:MM:SS" or "YYYY-MM-DD HH:MM:SS"
            dt_str = dt_str.replace(", ", " ").replace(",", " ").strip()
            try:
                dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    dt = datetime.strptime(dt_str, "%Y%m%d")
                except ValueError:
                    try:
                        dt = datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S")
                    except ValueError:
                        try:
                            dt = datetime.strptime(dt_str, "%Y-%m-%d")
                        except ValueError:
                            raise ValueError(f"Unknown date format: '{dt_str}'")

            fill_time = dt.strftime("%y%m%d-%H:%M:%S")
            key = f"csv_{sym}_{fill_time}_{qty}_{price}"

            rec = {
                "key": key,
                "orderId": 0,
                "permId": 0,
                "symbol": sym,
                "conid": 0,
                "side": side,
                "qty": qty,
                "avgPrice": round(price, 4),
                "commission": round(abs(comm), 4),
                "realizedPnl": round(pnl, 6),
                "fillTime": fill_time,
                "orderType": "CSV",
                "status": "Filled",
            }
            if _journal_add_trade(rec):
                added += 1
        except Exception as ex:
            errors.append(str(ex))
            if len(errors) > 10:
                break

    return web.json_response(
        {
            "added": added,
            "total": len(_trade_journal),
            "errors": errors[:5],
            "parsed_rows": len(trade_lines),
            "skipped": skipped,
            "col_map": col_map,
            "headers": (header_row or [])[:12],
            "sample": sample_row[:12] if sample_row else [],
            "accountName": account_name,
        }
    )


# ── Tradability check (whatIf order) ──────────────────────────
_tradable_cache: dict = {}  # conid -> bool (True=tradable, False=NT)


async def h_check_tradable(req):
    """POST /check-tradable  body: {conids: [123, 456, ...]}"""
    body = await req.json()
    cids = body.get("conids", [])
    if not cids:
        return web.json_response({})
    # Return cached results immediately, queue unknowns for background check
    result = {}
    need = []
    for cid in cids:
        cid_str = str(cid)
        if cid_str in _tradable_cache:
            result[cid_str] = _tradable_cache[cid_str]
        else:
            need.append(cid_str)
    if need:
        try:
            checked = await _await_ib(_check_tradable_batch(need), timeout=20)
            result.update(checked)
        except Exception as e:
            log.warning(f"Tradability check failed: {e}")
    return web.json_response(result)


async def _check_tradable_batch(cids):
    """Check tradability for a batch of conids using whatIfOrder."""
    out = {}
    for cid in cids:
        if cid in _tradable_cache:
            out[cid] = _tradable_cache[cid]
            continue
        try:
            contract = await _resolve_contract(cid)
            if not contract:
                _tradable_cache[cid] = False
                out[cid] = False
                continue
            contract_copy = Contract(
                conId=contract.conId,
                symbol=contract.symbol,
                secType=contract.secType,
                exchange="SMART",
                currency=contract.currency or "USD",
            )
            order = LimitOrder("BUY", 1, 0.01)
            order.whatIf = True
            trade = ib.placeOrder(contract_copy, order)
            # Wait briefly for response
            for _ in range(8):
                await asyncio.sleep(0.25)
                if trade.orderStatus.status:
                    break
            status = trade.orderStatus.status if trade.orderStatus else ""
            # whatIf orders return status with margin info or errors
            # If we get an error (Inactive) or specific rejection, it's NT
            oid = trade.order.orderId
            err = _order_errors.pop(oid, None)
            is_tradable = True
            if status == "Inactive" or err:
                # Check for permission-related errors
                if err and any(
                    kw in err.lower()
                    for kw in [
                        "permission",
                        "not available",
                        "cannot trade",
                        "restricted",
                        "no trading",
                        "not allowed",
                        "sec rule",
                        "regulation",
                    ]
                ):
                    is_tradable = False
                # Non-permission errors (e.g. margin) still mean tradable in principle
            _tradable_cache[cid] = is_tradable
            out[cid] = is_tradable
            # Cancel the whatIf order if it somehow went through
            try:
                ib.cancelOrder(trade.order)
            except Exception:
                pass
        except Exception as e:
            log.warning(f"Tradability check for {cid}: {e}")
            _tradable_cache[cid] = True  # assume tradable on error
            out[cid] = True
    return out


async def h_search(req):
    body = await req.json()
    sym = body.get("symbol", "").upper().strip()
    if not sym:
        return web.json_response([])
    try:
        result = await _await_ib(_search(sym), timeout=10)
    except Exception as e:
        return web.json_response([])
    return web.json_response(result)


async def _search(sym):
    c = Stock(sym, "SMART", "USD")
    try:
        details = await ib.reqContractDetailsAsync(c)
        out = []
        for d in details or []:
            cc = d.contract
            _conid_to_contract[str(cc.conId)] = cc
            out.append(
                {
                    "conid": cc.conId,
                    "symbol": cc.symbol,
                    "companyName": d.longName,
                    "companyHeader": f"{cc.symbol} - {d.longName}",
                    "secType": cc.secType,
                }
            )
        return out
    except Exception as e:
        return []


async def h_accounts(req):
    return web.json_response(
        [{"accountId": ib_account, "id": ib_account}] if ib_account else []
    )


async def h_positions(req):
    try:
        result = await _await_ib(_positions(), timeout=10)
    except Exception as e:
        return web.json_response([])
    return web.json_response(result)


async def _positions():
    out = []
    for p in ib.positions(account=ib_account):
        c = p.contract
        # Get live market price from streaming MD if available
        cid = str(c.conId)
        mkt_price = 0
        md = _conid_to_md_ticker.get(cid)
        if md:
            mp = md.marketPrice()
            if mp is not None and mp == mp:  # not NaN
                mkt_price = mp
            elif _is_num(md.last):
                mkt_price = md.last
        avg_price = p.avgCost / (100 if c.secType == "OPT" else 1)
        qty = p.position
        upnl = (mkt_price - avg_price) * qty if mkt_price else 0
        out.append(
            {
                "conid": c.conId,
                "position": qty,
                "pos": qty,
                "ticker": c.symbol,
                "symbol": c.symbol,
                "avgCost": p.avgCost,
                "avgPrice": avg_price,
                "mktPrice": mkt_price,
                "unrealizedPnl": upnl,
            }
        )
    return out


async def h_summary(req):
    try:
        result = await _await_ib(_summary(), timeout=10)
    except Exception as e:
        return web.json_response({})
    return web.json_response(result)


async def _summary():
    out = {}
    currency = ""
    # 1) accountValues with account filter (from reqAccountUpdates subscription)
    avs = ib.accountValues(account=ib_account)
    for v in avs:
        if v.tag == "NetLiquidation":
            out["netliquidationvalue"] = v.value
            out["NetLiquidation"] = v.value
            if v.currency:
                currency = v.currency
            break
    # 2) accountValues without filter
    if "netliquidationvalue" not in out:
        avs2 = ib.accountValues()
        for v in avs2:
            if v.tag == "NetLiquidation":
                out["netliquidationvalue"] = v.value
                out["NetLiquidation"] = v.value
                if v.currency:
                    currency = v.currency
                break
    # 3) accountSummary cache
    if "netliquidationvalue" not in out:
        asv = ib.accountSummary()
        for v in asv:
            if v.tag == "NetLiquidation":
                out["netliquidationvalue"] = v.value
                out["NetLiquidation"] = v.value
                if v.currency:
                    currency = v.currency
                break
    # 4) fresh reqAccountSummaryAsync
    if "netliquidationvalue" not in out:
        try:
            vals = await ib.reqAccountSummaryAsync()
            for v in vals or []:
                if v.tag == "NetLiquidation":
                    out["netliquidationvalue"] = v.value
                    out["NetLiquidation"] = v.value
                    if v.currency:
                        currency = v.currency
                    break
        except Exception:
            pass
    out["currency"] = currency
    return out


async def h_ledger(req):
    try:
        result = await _await_ib(_summary(), timeout=10)
    except Exception:
        result = {}
    nlv = 0
    v = result.get("netliquidationvalue", 0)
    if v:
        try:
            nlv = float(v)
        except (ValueError, TypeError):
            pass
    return web.json_response(
        {"currency": result.get("currency", "USD"), "netliquidationvalue": nlv}
    )


async def _cached_executions():
    """Return reqExecutionsAsync() result, cached for _EXEC_CACHE_TTL seconds."""
    global _exec_cache, _exec_cache_ts
    now = time.time()
    if _exec_cache is not None and (now - _exec_cache_ts) < _EXEC_CACHE_TTL:
        return _exec_cache
    _exec_cache = await ib.reqExecutionsAsync()
    _exec_cache_ts = now
    return _exec_cache


async def h_pnl(req):
    try:
        result = await _await_ib(_pnl(), timeout=10)
    except Exception as e:
        return web.json_response({"upnl": {}, "dpl": 0})
    return web.json_response(result)


async def _pnl():
    today = date.today()
    pnls = ib.pnl(ib_account)
    dp = pnls[0].dailyPnL if pnls and pnls[0].dailyPnL else 0
    rp = pnls[0].realizedPnL if pnls and pnls[0].realizedPnL else 0
    up = pnls[0].unrealizedPnL if pnls and pnls[0].unrealizedPnL else 0
    # If TWS dailyPnL may be stale (from previous trading day),
    # check if we have any fills today by local date — if not, reset to 0
    if dp:
        try:
            fills = await _cached_executions()
            has_today = any(
                f.execution.time
                and hasattr(f.execution.time, "date")
                and f.execution.time.date() >= today
                for f in (fills or [])
            )
            if not has_today:
                dp = 0
                rp = 0
                up = 0
        except Exception:
            pass
    return {"upnl": {ib_account: {"dpl": dp, "rpl": rp, "upl": up, "el": 0}}, "dpl": dp}


async def h_orders(req):
    try:
        result = await _await_ib(_orders(), timeout=20)

    except Exception as e:
        return web.json_response({"orders": []})
    return web.json_response({"orders": result})


async def _orders():
    out = []
    seen_oids = set()

    def _n(v):
        try:
            return float(v)
        except Exception:
            return 0.0

    # 1) Fetch today's executions (fills) — has real fill prices & times
    exec_by_oid = {}  # orderId -> {avgPrice, cumQty, side, symbol, conid, time}
    exec_by_perm = {}  # permId  -> same
    try:
        fills = await _cached_executions()
        for f in fills or []:
            e = f.execution
            key_oid = e.orderId
            key_perm = e.permId
            comm = f.commissionReport.commission if f.commissionReport else 0
            if comm and comm > 1e8:
                comm = 0  # UNSET_DOUBLE sentinel
            rec = {
                "avgPrice": e.avgPrice,
                "cumQty": e.cumQty,
                "side": e.side,
                "shares": e.shares,
                "price": e.price,
                "symbol": f.contract.symbol,
                "conid": f.contract.conId,
                "time": e.time,
                "orderId": e.orderId,
                "permId": e.permId,
                "commission": comm,
            }
            # Aggregate commission per orderId
            if key_oid and key_oid in exec_by_oid:
                exec_by_oid[key_oid]["commission"] = (
                    exec_by_oid[key_oid].get("commission", 0) + comm
                )
                exec_by_oid[key_oid]["avgPrice"] = e.avgPrice  # latest cumulative
            else:
                if key_oid:
                    exec_by_oid[key_oid] = rec
            if key_perm and key_perm not in exec_by_perm:
                exec_by_perm[key_perm] = rec
    except Exception as ex:
        log.warning(f"reqExecutions failed: {ex}")

    # ── Compute realized P&L per orderId from execution fills ──
    # Group individual fills by conid, process in time order, track avg cost
    from collections import defaultdict

    _fills_by_conid = defaultdict(list)
    for f in fills or []:
        e = f.execution
        cm = f.commissionReport.commission if f.commissionReport else 0
        if cm and cm > 1e8:
            cm = 0
        _fills_by_conid[f.contract.conId].append(
            {
                "orderId": e.orderId,
                "side": e.side,
                "shares": e.shares,
                "price": e.price,
                "time": e.time,
                "commission": cm,
            }
        )
    _realized_pnl.clear()
    _pnl_tmp = defaultdict(float)  # orderId -> realized P&L (exit - entry) * qty
    for _conid, cfills in _fills_by_conid.items():
        cfills.sort(key=lambda x: (x["time"] or "", x["orderId"]))
        pos = 0.0  # positive = long, negative = short
        cost = 0.0  # total cost of current position
        for fl in cfills:
            qty = fl["shares"]
            px = fl["price"]
            is_buy = fl["side"] in ("BOT", "BUY")
            if pos > 0 and not is_buy:
                # Closing long
                close_qty = min(qty, pos)
                avg = cost / pos if pos else 0
                _pnl_tmp[fl["orderId"]] += (px - avg) * close_qty - fl["commission"]
                cost -= avg * close_qty
                pos -= close_qty
                leftover = qty - close_qty
                if leftover > 0:
                    pos -= leftover
                    cost -= px * leftover
            elif pos < 0 and is_buy:
                # Closing short
                close_qty = min(qty, abs(pos))
                avg = abs(cost / pos) if pos else 0
                _pnl_tmp[fl["orderId"]] += (avg - px) * close_qty - fl["commission"]
                cost += avg * close_qty
                pos += close_qty
                leftover = qty - close_qty
                if leftover > 0:
                    pos += leftover
                    cost += px * leftover
            else:
                # Opening / adding to position — commission is a sunk cost, not realized yet
                if is_buy:
                    pos += qty
                    cost += px * qty
                else:
                    pos -= qty
                    cost -= px * qty

    _realized_pnl.update(_pnl_tmp)

    # 2) All open orders (includes orders placed from other sessions/clients)
    try:
        open_trades = await ib.reqAllOpenOrdersAsync()
        for t in open_trades or []:
            o, c, st = t.order, t.contract, t.orderStatus
            if getattr(o, "whatIf", False):
                continue
            oid = o.orderId or o.permId
            if oid in seen_oids:
                continue
            seen_oids.add(oid)
            if o.permId:
                seen_oids.add(o.permId)
            order_time = ""
            if t.log:
                lt = t.log[-1].time
                if lt:
                    order_time = (
                        lt.strftime("%y%m%d-%H:%M:%S")
                        if hasattr(lt, "strftime")
                        else str(lt)
                    )
            ex_rec = exec_by_oid.get(o.orderId) or exec_by_perm.get(o.permId)
            commission = ex_rec["commission"] if ex_rec else 0
            out.append(
                {
                    "orderId": o.orderId,
                    "id": o.orderId,
                    "permId": o.permId,
                    "conid": c.conId,
                    "symbol": c.symbol,
                    "ticker": c.symbol,
                    "contractDesc": c.symbol,
                    "side": o.action,
                    "totalSize": o.totalQuantity,
                    "filledQuantity": st.filled if st else 0,
                    "remainingQuantity": st.remaining if st else o.totalQuantity,
                    "orderType": o.orderType,
                    "price": o.lmtPrice if o.orderType == "LMT" else 0,
                    "auxPrice": o.auxPrice if hasattr(o, "auxPrice") and o.auxPrice else 0,
                    "avgPrice": st.avgFillPrice if st else 0,
                    "status": st.status if st else "Unknown",
                    "lastExecutionTime": "",
                    "orderTime": order_time,
                    "commission": commission,
                    "realizedPnl": round(_realized_pnl.get(o.orderId, 0), 2),
                }
            )
    except Exception as ex:
        log.warning(f"reqAllOpenOrders failed: {ex}")

    # 3) Current session trades (filled/cancelled from this session)
    for t in ib.trades():
        o, c, st = t.order, t.contract, t.orderStatus
        if getattr(o, "whatIf", False):
            continue
        oid = o.orderId or o.permId
        if oid in seen_oids:
            continue
        seen_oids.add(oid)
        # Get last fill time if available
        fill_time = ""
        if t.fills:
            ft = t.fills[-1].time
            if ft:
                fill_time = (
                    ft.strftime("%y%m%d-%H:%M:%S")
                    if hasattr(ft, "strftime")
                    else str(ft)
                )
        # Fallback: use last log entry time
        order_time = ""
        if t.log:
            lt = t.log[-1].time
            if lt:
                order_time = (
                    lt.strftime("%y%m%d-%H:%M:%S")
                    if hasattr(lt, "strftime")
                    else str(lt)
                )
        # Fill price: prefer orderStatus, fallback to executions, fallback to computing from fills
        avg_price = st.avgFillPrice if st else 0
        if not avg_price:
            ex = exec_by_oid.get(o.orderId) or exec_by_perm.get(o.permId)
            if ex:
                avg_price = ex["avgPrice"]
                if not fill_time and ex["time"]:
                    ft2 = ex["time"]
                    fill_time = (
                        ft2.strftime("%y%m%d-%H:%M:%S")
                        if hasattr(ft2, "strftime")
                        else str(ft2)
                    )
        if not avg_price and t.fills:
            total_qty = sum(f.execution.shares for f in t.fills)
            if total_qty > 0:
                avg_price = (
                    sum(f.execution.price * f.execution.shares for f in t.fills)
                    / total_qty
                )
        # Commission: from executions lookup or sum from fills
        commission = 0
        ex_rec = exec_by_oid.get(o.orderId) or exec_by_perm.get(o.permId)
        if ex_rec:
            commission = ex_rec.get("commission", 0)
        if not commission and t.fills:
            for f in t.fills:
                c2 = f.commissionReport.commission if f.commissionReport else 0
                if c2 and c2 < 1e8:
                    commission += c2
        out.append(
            {
                "orderId": o.orderId,
                "id": o.orderId,
                "permId": o.permId,
                "conid": c.conId,
                "symbol": c.symbol,
                "ticker": c.symbol,
                "contractDesc": c.symbol,
                "side": o.action,
                "totalSize": o.totalQuantity,
                "filledQuantity": st.filled if st else 0,
                "remainingQuantity": st.remaining if st else o.totalQuantity,
                "orderType": o.orderType,
                "price": o.lmtPrice if o.orderType == "LMT" else 0,
                "auxPrice": o.auxPrice if hasattr(o, "auxPrice") and o.auxPrice else 0,
                "avgPrice": avg_price,
                "status": st.status if st else "Unknown",
                "lastExecutionTime": fill_time,
                "orderTime": order_time,
                "commission": round(commission, 4),
                "realizedPnl": round(_realized_pnl.get(o.orderId, 0), 2),
            }
        )

    _step23 = len(out)
    log.info(
        f"_orders step2+3: {_step23} orders from open+trades, seen_oids={len(seen_oids)}"
    )

    # 4) Add any executions not already covered (e.g. from before reconnect)
    _step4_added = 0
    for key, ex in exec_by_oid.items():
        perm = ex.get("permId", 0)
        if key in seen_oids:
            continue
        if perm and perm in seen_oids:
            # A row for this permId already exists; enrich it with execution details
            # so we don't lose real orderId/fill timestamps to a sparse orderId=0 row.
            for rec in out:
                if int(_n(rec.get("permId"))) != int(_n(perm)):
                    continue
                if not _n(rec.get("orderId")):
                    rec["orderId"] = key
                    rec["id"] = key
                if not _n(rec.get("totalSize")):
                    rec["totalSize"] = ex["cumQty"]
                if not _n(rec.get("filledQuantity")):
                    rec["filledQuantity"] = ex["cumQty"]
                if not _n(rec.get("avgPrice")):
                    rec["avgPrice"] = ex["avgPrice"]
                if not _n(rec.get("price")):
                    rec["price"] = ex["price"]
                if ex.get("time") and not rec.get("lastExecutionTime"):
                    ft2 = ex["time"]
                    fill_time2 = (
                        ft2.strftime("%y%m%d-%H:%M:%S")
                        if hasattr(ft2, "strftime")
                        else str(ft2)
                    )
                    rec["lastExecutionTime"] = fill_time2
                    if not rec.get("orderTime"):
                        rec["orderTime"] = fill_time2
                rec["commission"] = round(
                    max(_n(rec.get("commission")), _n(ex.get("commission", 0))), 4
                )
                rec["realizedPnl"] = round(
                    _realized_pnl.get(key, _n(rec.get("realizedPnl"))), 2
                )
                if _n(rec.get("filledQuantity")) > 0 and rec.get("status") in (
                    "Unknown",
                    "Inactive",
                ):
                    rec["status"] = "Filled"
                break
            seen_oids.add(key)
            continue
        seen_oids.add(key)
        if perm:
            seen_oids.add(perm)
        ft = ex["time"]
        fill_time = (
            ft.strftime("%y%m%d-%H:%M:%S")
            if hasattr(ft, "strftime")
            else str(ft) if ft else ""
        )
        side = ex["side"]
        if side == "BOT":
            side = "BUY"
        elif side == "SLD":
            side = "SELL"
        out.append(
            {
                "orderId": key,
                "id": key,
                "permId": perm,
                "conid": ex["conid"],
                "symbol": ex["symbol"],
                "ticker": ex["symbol"],
                "contractDesc": ex["symbol"],
                "side": side,
                "totalSize": ex["cumQty"],
                "filledQuantity": ex["cumQty"],
                "remainingQuantity": 0,
                "orderType": "LMT",
                "price": ex["price"],
                "avgPrice": ex["avgPrice"],
                "status": "Filled",
                "lastExecutionTime": fill_time,
                "orderTime": fill_time,
                "fillTime": fill_time,
                "commission": round(ex.get("commission", 0), 4),
                "realizedPnl": round(_realized_pnl.get(key, 0), 2),
            }
        )
        _step4_added += 1

    log.info(
        f"_orders step4: added {_step4_added} filled from exec_by_oid, total={len(out)}"
    )

    # 5) Completed orders (cancelled, filled that dropped from ib.trades)
    try:
        completed = await ib.reqCompletedOrdersAsync(apiOnly=False)
        _comp_statuses = {}
        for _ct in completed or []:
            _cs = _ct.orderStatus.status if _ct.orderStatus else "?"
            _comp_statuses[_cs] = _comp_statuses.get(_cs, 0) + 1
        log.info(
            f"reqCompletedOrders returned {len(completed or [])} orders: {_comp_statuses}"
        )
        for t in completed or []:
            o, c, st = t.order, t.contract, t.orderStatus
            if getattr(o, "whatIf", False):
                continue
            log.info(
                f"  completed MERGE oid={o.orderId} perm={o.permId} status={st.status if st else '?'} sym={c.symbol}"
            )
            order_time = ""
            if t.log:
                lt = t.log[-1].time
                if lt:
                    order_time = (
                        lt.strftime("%y%m%d-%H:%M:%S")
                        if hasattr(lt, "strftime")
                        else str(lt)
                    )
            # Try to get fill price from executions lookup
            avg_price = st.avgFillPrice if st else 0
            fill_time = ""
            if not avg_price:
                ex = exec_by_oid.get(o.orderId) or exec_by_perm.get(o.permId)
                if ex:
                    avg_price = ex["avgPrice"]
                    if ex["time"]:
                        ft2 = ex["time"]
                        fill_time = (
                            ft2.strftime("%y%m%d-%H:%M:%S")
                            if hasattr(ft2, "strftime")
                            else str(ft2)
                        )
            if not order_time:
                order_time = fill_time
            comp_comm = 0
            comp_ex = exec_by_oid.get(o.orderId) or exec_by_perm.get(o.permId)
            if comp_ex:
                comp_comm = comp_ex.get("commission", 0)
            out.append(
                {
                    "orderId": o.orderId,
                    "id": o.orderId,
                    "permId": o.permId,
                    "conid": c.conId,
                    "symbol": c.symbol,
                    "ticker": c.symbol,
                    "contractDesc": c.symbol,
                    "side": o.action,
                    "totalSize": o.totalQuantity,
                    "filledQuantity": st.filled if st else 0,
                    "remainingQuantity": st.remaining if st else 0,
                    "orderType": o.orderType,
                    "price": o.lmtPrice if o.orderType == "LMT" else 0,
                    "avgPrice": avg_price,
                    "status": st.status if st else "Unknown",
                    "lastExecutionTime": fill_time,
                    "orderTime": order_time,
                    "commission": round(comp_comm, 4),
                    "realizedPnl": round(_realized_pnl.get(o.orderId, 0), 2),
                }
            )
    except Exception as ex:
        log.warning(f"reqCompletedOrders failed: {ex}")

    # Fix up statuses & quantities from execution data
    for rec in out:
        status = rec.get("status", "")
        if status == "ApiCancelled":
            rec["status"] = status = "Cancelled"
        filled = rec.get("filledQuantity", 0) or 0
        remaining = rec.get("remainingQuantity", 0)
        # Also check executions lookup for this orderId / permId
        oid = rec.get("orderId")
        ex = exec_by_oid.get(oid)
        if not ex:
            perm = rec.get("permId")
            if perm:
                ex = exec_by_perm.get(perm)
        # Ensure totalSize (qty) is always populated
        if not rec.get("totalSize") and ex:
            rec["totalSize"] = ex["cumQty"]
        # Ensure filledQuantity is populated for filled orders
        if status == "Filled" and not rec.get("filledQuantity"):
            if ex:
                rec["filledQuantity"] = ex["cumQty"]
            elif rec.get("totalSize"):
                rec["filledQuantity"] = rec["totalSize"]
        if status == "Inactive":
            if (filled and remaining == 0) or ex:
                rec["status"] = "Filled"
                if ex and not rec.get("avgPrice"):
                    rec["avgPrice"] = ex["avgPrice"]
                if ex and not rec.get("lastExecutionTime") and ex.get("time"):
                    ft = ex["time"]
                    rec["lastExecutionTime"] = (
                        ft.strftime("%y%m%d-%H:%M:%S")
                        if hasattr(ft, "strftime")
                        else str(ft)
                    )

    # 6) Canonical dedupe by logical identity
    # IB can emit duplicate logical orders across sources with orderId=0 but same permId.
    def _num(v):
        try:
            return float(v)
        except Exception:
            return 0.0

    def _status_rank(s):
        s = str(s or "")
        if s == "Filled":
            return 4
        if s in ("Cancelled", "Inactive"):
            return 3
        if s in ("PreSubmitted", "Submitted", "PendingSubmit"):
            return 2
        return 1

    def _row_score(r):
        score = 0
        if _num(r.get("orderId")) > 0:
            score += 100
        if _num(r.get("permId")) > 0:
            score += 50
        if r.get("lastExecutionTime"):
            score += 25
        if r.get("orderTime"):
            score += 15
        if _num(r.get("avgPrice")) > 0:
            score += 20
        if _num(r.get("filledQuantity")) > 0:
            score += 10
        if _num(r.get("commission")) > 0:
            score += 5
        score += _status_rank(r.get("status"))
        return score

    def _merge_rows(a, b):
        # Keep the stronger base row but preserve useful non-empty fields from both.
        if _row_score(a) >= _row_score(b):
            base, other = dict(a), b
        else:
            base, other = dict(b), a

        for k in ("lastExecutionTime", "orderTime", "fillTime"):
            bv = str(base.get(k) or "")
            ov = str(other.get(k) or "")
            if not bv and ov:
                base[k] = ov
            elif bv and ov and ov > bv:
                base[k] = ov

        # Preserve identity fields if missing in the selected base row.
        for k in ("orderId", "id", "permId", "conid"):
            if not _num(base.get(k)) and _num(other.get(k)):
                base[k] = other.get(k)

        # Keep larger qty/price style values when base is empty or smaller.
        for k in (
            "totalSize",
            "filledQuantity",
            "price",
            "avgPrice",
            "commission",
            "realizedPnl",
        ):
            if _num(base.get(k)) < _num(other.get(k)):
                base[k] = other.get(k)

        # Prefer more final status when both exist.
        if _status_rank(other.get("status")) > _status_rank(base.get("status")):
            base["status"] = other.get("status")

        return base

    def _recency_key(rec):
        return str(
            rec.get("lastExecutionTime")
            or rec.get("orderTime")
            or rec.get("fillTime")
            or ""
        )

    by_key = {}
    for rec in out:
        pid = int(_num(rec.get("permId")))
        oid = int(_num(rec.get("orderId")))
        if pid > 0:
            key = f"perm:{pid}"
        elif oid > 0:
            key = f"oid:{oid}"
        else:
            key = "fb:{}|{}|{}|{}|{}|{}|{}|{}".format(
                rec.get("symbol", ""),
                rec.get("side", ""),
                rec.get("totalSize", ""),
                rec.get("orderType", ""),
                rec.get("price", ""),
                rec.get("avgPrice", ""),
                rec.get("status", ""),
                rec.get("lastExecutionTime") or rec.get("orderTime") or "",
            )
        prev = by_key.get(key)
        if not prev:
            by_key[key] = rec
        else:
            by_key[key] = _merge_rows(prev, rec)
    out = list(by_key.values())

    # Sort by most recent first, falling back to orderId.
    out.sort(key=lambda o: (_recency_key(o), _num(o.get("orderId"))), reverse=True)
    statuses = {}
    for o in out:
        s = o.get("status", "?")
        statuses[s] = statuses.get(s, 0) + 1
    log.info(f"h_orders returning {len(out)} orders: {statuses}")
    return out


async def h_place_order(req):
    body = await req.json()
    ol = body.get("orders", [])
    if not ol:
        return web.json_response([{"error": "No orders"}])
    o = ol[0]
    try:
        result = await _await_ib(
            _place(
                str(o.get("conid", "")),
                o.get("side", "BUY"),
                o.get("quantity", 0),
                o.get("orderType", "LMT"),
                o.get("price", 0),
                o.get("tif", "DAY"),
                o.get("outsideRTH", True),
                o.get("ocaGroup", ""),
                o.get("ocaType", 0),
                o.get("waitForFill", True),
                tp_price=o.get("tpPrice") or None,
                sl_price=o.get("slPrice") or None,
            ),
            timeout=15,
        )
    except Exception as e:
        return web.json_response([{"error": str(e)}])
    return web.json_response(result)


async def _place(
    conid,
    side,
    qty,
    otype,
    price,
    tif,
    outside_rth=True,
    oca_group="",
    oca_type=0,
    wait_for_fill=True,
    tp_price=None,
    sl_price=None,
):
    contract = await _resolve_contract(conid)
    if not contract:
        return [{"error": f"Cannot resolve conid {conid}"}]
    contract.exchange = "SMART"
    exit_side = "SELL" if side == "BUY" else "BUY"
    has_bracket = tp_price or sl_price
    # Build parent order (transmit=False when bracket children follow)
    if otype == "MKT":
        parent = MarketOrder(side, qty)
    else:
        parent = LimitOrder(side, qty, price)
    parent.tif = tif
    parent.outsideRth = outside_rth
    if oca_group:
        parent.ocaGroup = str(oca_group)
        parent.ocaType = int(oca_type) if oca_type else 1
    if has_bracket:
        parent.transmit = False
    _order_errors.pop(0, None)  # clear stale
    trade = ib.placeOrder(contract, parent)
    oid = trade.order.orderId
    # Place bracket children linked by parentId
    tp_oid = None
    sl_oid = None
    if has_bracket:
        children = []
        if tp_price:
            tp_order = LimitOrder(exit_side, qty, float(tp_price))
            tp_order.parentId = oid
            tp_order.tif = "DAY"
            tp_order.transmit = not bool(sl_price)  # last child transmits all
            children.append((tp_order, "tp"))
        if sl_price:
            sl_order = StopOrder(exit_side, qty, float(sl_price))
            sl_order.parentId = oid
            sl_order.tif = "DAY"
            sl_order.transmit = True  # always last
            children.append((sl_order, "sl"))
        for child_order, child_type in children:
            child_trade = ib.placeOrder(contract, child_order)
            if child_type == "tp":
                tp_oid = child_trade.order.orderId
            else:
                sl_oid = child_trade.order.orderId
    # Brief pause for TWS to ack (reject/inactive arrives fast).
    await asyncio.sleep(0.15)
    status = trade.orderStatus.status if trade.orderStatus else "Submitted"
    result = {"order_id": oid, "order_status": status}
    if tp_oid:
        result["tp_order_id"] = tp_oid
    if sl_oid:
        result["sl_order_id"] = sl_oid
    # Get fill price from actual executions (most accurate, no commission)
    if trade.fills:
        total_qty = sum(f.execution.shares for f in trade.fills)
        total_val = sum(f.execution.shares * f.execution.price for f in trade.fills)
        if total_qty > 0:
            result["avgFillPrice"] = round(total_val / total_qty, 4)
    # Fallback to orderStatus.avgFillPrice
    if (
        "avgFillPrice" not in result
        and trade.orderStatus
        and trade.orderStatus.avgFillPrice
    ):
        result["avgFillPrice"] = trade.orderStatus.avgFillPrice
    log.info(
        f"Order {oid}: status={status} avgFillPrice={result.get('avgFillPrice','N/A')} fills={len(trade.fills)}"
    )
    # Attach error message if order was rejected/inactive
    if status == "Inactive":
        err = _order_errors.pop(oid, None)
        result["error"] = (
            err or "Order rejected by TWS (Inactive). Check TWS for details."
        )
    return [result]


async def h_reply(req):
    # TWS doesn't need Client Portal confirmation dance
    return web.json_response([{"order_id": 0, "message": "confirmed"}])


async def h_cancel_order(req):
    oid = int(req.match_info.get("orderId", 0))
    try:
        result = await _await_ib(_cancel(oid), timeout=10)
    except Exception as e:
        return web.json_response({"error": str(e)})
    if not result:
        return web.json_response({"error": f"Order {oid} not found or not cancellable"})
    return web.json_response(result)


async def _cancel(oid):
    for t in ib.trades():
        if t.order.orderId == oid:
            ib.cancelOrder(t.order)
            return {"msg": "cancelled", "order_id": oid}
    try:
        open_trades = await ib.reqAllOpenOrdersAsync()
    except Exception:
        open_trades = []
    for t in open_trades or []:
        if getattr(t.order, "orderId", 0) == oid:
            ib.cancelOrder(t.order)
            return {"msg": "cancelled", "order_id": oid}
    return None


async def h_modify_order(req):
    oid = int(req.match_info.get("orderId", 0))
    body = await req.json()
    new_price = body.get("price", 0)
    aux_price = body.get("auxPrice", None)
    if not new_price and not aux_price:
        return web.json_response({"error": "price required"})
    try:
        result = await _await_ib(_modify(oid, new_price or 0, aux_price), timeout=10)
    except Exception as e:
        return web.json_response({"error": str(e)})
    return web.json_response(result or {"msg": "modified"})


async def _modify(oid, new_price, aux_price=None):
    for t in ib.trades():
        if t.order.orderId == oid:
            order = t.order
            if aux_price is not None:
                order.auxPrice = float(aux_price)
            elif order.orderType in ("STP", "STP LMT"):
                order.auxPrice = float(new_price)
            else:
                order.lmtPrice = float(new_price)
            try:
                ib.placeOrder(t.contract, order)
            except Exception as e:
                return {"error": str(e)}
            return {"order_id": oid, "new_price": float(new_price)}
    return {"error": f"Order {oid} not found"}


# ── External proxies & TWS news ──────────────────────────────


async def h_float(req):
    ticker = req.match_info.get("ticker", "").upper().strip()
    if not re.match(r"^[A-Z0-9.\-]{1,10}$", ticker):
        return web.json_response({"ticker": ticker, "float": None}, status=400)
    if ticker in _float_cache:
        return web.json_response(
            {
                "ticker": ticker,
                "float": _float_cache[ticker],
                "country": _country_cache.get(ticker),
            }
        )
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _fetch_float_yf, [ticker])
    r = result.get(ticker, {})
    return web.json_response(
        {"ticker": ticker, "float": r.get("float"), "country": r.get("country")}
    )


async def h_floats_batch(req):
    """Batch float+country lookup via urllib Yahoo API."""
    try:
        body = await req.json()
    except Exception:
        return web.json_response({}, status=400)
    tickers = body.get("tickers", [])
    if not isinstance(tickers, list):
        return web.json_response({}, status=400)
    # Validate and dedupe
    clean = list(
        dict.fromkeys(
            t.upper().strip()
            for t in tickers
            if isinstance(t, str) and re.match(r"^[A-Z0-9.\-]{1,10}$", t.strip())
        )
    )
    if not clean:
        return web.json_response({})
    # Return cached results immediately, fetch missing
    result = {}
    need = []
    for t in clean:
        if t in _float_cache:
            result[t] = {"float": _float_cache[t], "country": _country_cache.get(t)}
        else:
            need.append(t)
    if need:
        loop = asyncio.get_event_loop()
        fetched = await loop.run_in_executor(None, _fetch_float_yf, need)
        result.update(fetched)
    return web.json_response(result)


# Country name -> ISO 2-letter code map
_COUNTRY_ISO = {
    "usa": "US",
    "united states": "US",
    "china": "CN",
    "hong kong": "HK",
    "canada": "CA",
    "united kingdom": "GB",
    "israel": "IL",
    "ireland": "IE",
    "japan": "JP",
    "south korea": "KR",
    "taiwan": "TW",
    "india": "IN",
    "brazil": "BR",
    "mexico": "MX",
    "germany": "DE",
    "france": "FR",
    "netherlands": "NL",
    "switzerland": "CH",
    "australia": "AU",
    "singapore": "SG",
    "bermuda": "BM",
    "cayman islands": "KY",
    "argentina": "AR",
    "chile": "CL",
    "colombia": "CO",
    "peru": "PE",
    "south africa": "ZA",
    "italy": "IT",
    "spain": "ES",
    "sweden": "SE",
    "norway": "NO",
    "denmark": "DK",
    "finland": "FI",
    "belgium": "BE",
    "luxembourg": "LU",
    "monaco": "MC",
    "greece": "GR",
    "turkey": "TR",
    "russia": "RU",
    "indonesia": "ID",
    "malaysia": "MY",
    "thailand": "TH",
    "philippines": "PH",
    "new zealand": "NZ",
    "macau": "MO",
    "uruguay": "UY",
    "panama": "PA",
    "cyprus": "CY",
    "malta": "MT",
    "jersey": "JE",
    "guernsey": "GG",
    "isle of man": "IM",
    "curacao": "CW",
    "british virgin islands": "VG",
    "marshall islands": "MH",
    "puerto rico": "PR",
    "netherlands antilles": "AN",
}


# Yahoo urllib fallback (no curl_cffi needed) — uses crumb+cookie auth
_yf_opener = None
_yf_crumb = None
_yf_crumb_ts = 0


def _yf_ensure_crumb():
    """Obtain a Yahoo Finance crumb+cookie pair (valid ~hours). Cached."""
    global _yf_opener, _yf_crumb, _yf_crumb_ts
    if _yf_crumb and time.time() - _yf_crumb_ts < 3600:
        return True
    _UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    try:
        import http.cookiejar

        cj = http.cookiejar.CookieJar()
        _yf_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
        # Hit fc.yahoo.com to get consent cookies (expect 404)
        try:
            _yf_opener.open(
                urllib.request.Request(
                    "https://fc.yahoo.com/", headers={"User-Agent": _UA}
                ),
                timeout=5,
            )
        except Exception:
            pass
        # Fetch crumb
        req = urllib.request.Request(
            "https://query2.finance.yahoo.com/v1/test/getcrumb",
            headers={"User-Agent": _UA},
        )
        _yf_crumb = _yf_opener.open(req, timeout=5).read().decode()
        _yf_crumb_ts = time.time()
        return bool(_yf_crumb)
    except Exception as e:
        log.warning("Yahoo crumb fetch failed: %s", e)
        return False


def _fetch_float_urllib(tickers):
    """Fallback: fetch float+country via Yahoo Finance quoteSummary (stdlib only)."""
    if not _yf_ensure_crumb():
        for t in tickers:
            _float_cache[t] = None
        return {t: {"float": None, "country": None} for t in tickers}
    _UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    result = {}
    for t in tickers:
        fv, cc = None, None
        try:
            url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{t}?modules=defaultKeyStatistics,assetProfile&crumb={_yf_crumb}"
            req = urllib.request.Request(url, headers={"User-Agent": _UA})
            with _yf_opener.open(req, timeout=10) as resp:
                data = json.loads(resp.read())
            qr = data.get("quoteSummary", {}).get("result", [])
            if qr:
                ks = qr[0].get("defaultKeyStatistics", {})
                raw = ks.get("floatShares", {}).get("raw")
                if isinstance(raw, (int, float)) and raw > 0:
                    fv = float(raw)
                ap = qr[0].get("assetProfile", {})
                country_name = ap.get("country", "")
                if country_name:
                    cc = _COUNTRY_ISO.get(
                        country_name.strip().lower(), country_name.strip()[:2].upper()
                    )
        except Exception as e:
            log.debug("urllib float fetch failed for %s: %s", t, e)
        _float_cache[t] = fv
        _country_cache[t] = cc
        result[t] = {"float": fv, "country": cc}
    return result


def _fetch_float_yf(tickers):
    """Fetch float + country for a list of tickers via urllib Yahoo API."""
    return _fetch_float_urllib(tickers)


_splits_cache: dict = {}  # symbol -> {"splits": [...], "ts": epoch}


def _fetch_splits_yf(ticker):
    """Fetch stock split history. Not available via urllib, returns empty."""
    # Stock splits only available via paid APIs or web scraping
    return []


async def h_splits(req):
    ticker = req.match_info.get("ticker", "").upper().strip()
    if not re.match(r"^[A-Z0-9.\-]{1,10}$", ticker):
        return web.json_response({"ticker": ticker, "splits": []}, status=400)
    cached = _splits_cache.get(ticker)
    if cached and (time.time() - cached["ts"]) < 86400:  # cache 24h
        return web.json_response({"ticker": ticker, "splits": cached["splits"]})
    loop = asyncio.get_event_loop()
    splits = await loop.run_in_executor(None, _fetch_splits_yf, ticker)
    _splits_cache[ticker] = {"splits": splits, "ts": time.time()}
    return web.json_response({"ticker": ticker, "splits": splits})


_news_conid_cache: dict = {}  # symbol -> conId
_news_providers: str = ""  # cached provider string like "BRFG+BRFUPDN+DJNL+BZ+FLY"
_contract_info_cache: dict = {}  # symbol -> {longName, industry, category, ...}
_yahoo_kpi_cache: dict = {}  # symbol -> {summary, kpis dict}
_shelf_cache: dict = {}  # symbol -> {"hasShelf": bool, ...}
_cik_map: dict = {}  # ticker -> CIK string
_cik_map_loaded: bool = False


async def _fetch_ib_kpis(ticker):
    """Fetch company KPIs + summary via IB fundamental data (no external network)."""
    if ticker in _yahoo_kpi_cache:
        return _yahoo_kpi_cache[ticker]
    result = {"summary": "", "kpis": {}}
    contract = Stock(ticker, "SMART", "USD")

    # ── 1. ReportSnapshot for business summary, employees, sector ──
    try:
        xml_str = await asyncio.wait_for(
            _await_ib(
                ib.reqFundamentalDataAsync(contract, "ReportSnapshot"), timeout=10
            ),
            timeout=12,
        )
        if xml_str:
            root = ET.fromstring(xml_str)
            # Business summary
            for txt in root.iter("Text"):
                if txt.get("Type") == "Business Summary" and txt.text:
                    result["summary"] = txt.text.strip()
                    break
            # Employees
            emp_el = root.find(".//Employees")
            if emp_el is not None and emp_el.text:
                try:
                    result["kpis"]["employees"] = int(emp_el.text)
                except ValueError:
                    pass
            # Shares outstanding (in thousands in XML)
            so_el = root.find(".//SharesOut")
            if so_el is not None and so_el.text:
                try:
                    result["kpis"]["sharesOut"] = float(so_el.text) * 1000
                except ValueError:
                    pass
            # Ratios from ReportSnapshot XML
            _NM = -99999.99
            _rs_map = {
                "MKTCAP": "marketCap",
                "PEEXCLXOR": "pe",
                "APENORM": "forwardPE",
                "TTMEPSXCLX": "eps",
                "BETA": "beta",
                "NHIG": "high52w",
                "NLOW": "low52w",
                "TTMREV": "revenue",
                "TTMREVCHG": "revenueGrowth",
                "TTMNPMGN": "profitMargin",
                "TTMROEPCT": "roe",
                "TTMFCF": "freeCashflow",
                "YIELD": "divYield",
                "EV_Cur": "enterpriseValue",
                "EPSTRENDGR": "earningsGrowth",
                "PR2TANBK": "pegRatio",
            }
            found_ratios = []
            for ratio in root.iter("Ratio"):
                fn = ratio.get("FieldName", "")
                txt = (ratio.text or "").strip()
                if not txt:
                    continue
                found_ratios.append(fn)
                try:
                    v = float(txt)
                except ValueError:
                    continue
                if v == _NM:
                    continue
                dst = _rs_map.get(fn)
                if dst:
                    # Convert percentages to decimals for consistency with frontend
                    if dst in (
                        "revenueGrowth",
                        "profitMargin",
                        "roe",
                        "divYield",
                        "earningsGrowth",
                    ):
                        v = v / 100.0
                    if dst in (
                        "marketCap",
                        "revenue",
                        "freeCashflow",
                        "enterpriseValue",
                    ):
                        v = v * 1_000_000  # IB reports in millions
                    result["kpis"][dst] = v
            log.info(
                "IB ReportSnapshot for %s: summary=%d chars, kpis=%s, xml_ratios_found=%d (%s)",
                ticker,
                len(result["summary"]),
                list(result["kpis"].keys()),
                len(found_ratios),
                found_ratios[:20],
            )
    except Exception as e:
        log.info("IB ReportSnapshot unavailable for %s: %s", ticker, e)

    # ── 2. Fallback: fundamentalRatios via reqMktData tick 258 ──
    if not result["kpis"].get("marketCap"):
        try:

            async def _req_fund_ratios():
                t = ib.reqMktData(contract, genericTickList="258", snapshot=True)
                await asyncio.sleep(3)  # wait for data to arrive
                fr = t.fundamentalRatios
                ib.cancelMktData(contract)
                return fr

            fr = await asyncio.wait_for(
                _await_ib(_req_fund_ratios(), timeout=8), timeout=10
            )
            if fr:
                d = vars(fr) if hasattr(fr, "__dict__") else {}
                _fr_map = {
                    "MKTCAP": "marketCap",
                    "PEEXCLXOR": "pe",
                    "APENORM": "forwardPE",
                    "TTMEPSXCLX": "eps",
                    "BETA": "beta",
                    "NHIG": "high52w",
                    "NLOW": "low52w",
                    "TTMREV": "revenue",
                    "TTMREVCHG": "revenueGrowth",
                    "TTMNPMGN": "profitMargin",
                    "TTMROEPCT": "roe",
                    "TTMFCF": "freeCashflow",
                    "YIELD": "divYield",
                    "EV_Cur": "enterpriseValue",
                }
                for ib_key, dst in _fr_map.items():
                    v = d.get(ib_key)
                    if (
                        isinstance(v, (int, float))
                        and v != -99999.99
                        and dst not in result["kpis"]
                    ):
                        if dst in ("revenueGrowth", "profitMargin", "roe", "divYield"):
                            v = v / 100.0
                        if dst in (
                            "marketCap",
                            "revenue",
                            "freeCashflow",
                            "enterpriseValue",
                        ):
                            v = v * 1_000_000
                        result["kpis"][dst] = v
                log.info("IB fundamentalRatios for %s: %s", ticker, list(d.keys())[:10])
        except Exception as e:
            log.info("IB fundamentalRatios unavailable for %s: %s", ticker, e)

    # ── 3. Supplement with urllib Yahoo API (fills missing fields IB doesn't provide) ──
    has_real_kpis = any(
        v for k, v in result["kpis"].items() if k not in ("sector", "industry")
    )
    if not has_real_kpis:
        log.info("IB had no KPI data for %s — trying urllib Yahoo fallback", ticker)
    loop = asyncio.get_event_loop()
    try:
        ydata = await asyncio.wait_for(
            loop.run_in_executor(None, _fetch_yahoo_stdlib, ticker), timeout=15
        )
        if isinstance(ydata, dict):
            if ydata.get("summary") and not result["summary"]:
                result["summary"] = ydata["summary"]
            if ydata.get("kpis"):
                for k, v in ydata["kpis"].items():
                    if k not in result["kpis"]:
                        result["kpis"][k] = v
    except Exception as e:
        log.info("urllib Yahoo supplement failed for %s: %s", ticker, e)

    # Cache only meaningful results
    has_real_kpis = any(
        v for k, v in result["kpis"].items() if k not in ("sector", "industry")
    )
    if result["summary"] or has_real_kpis:
        _yahoo_kpi_cache[ticker] = result
    else:
        log.warning("No fundamental data for %s from any source", ticker)
    return result


def _fetch_yahoo_stdlib(ticker):
    """Fetch Yahoo fundamentals via urllib API."""
    result = {"summary": "", "kpis": {}}
    if not _yf_ensure_crumb():
        return result

    _UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    try:
        # Fetch comprehensive modules for full KPI coverage
        modules = "assetProfile,defaultKeyStatistics,financialData,summaryDetail,insiderHoldings"
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules={modules}&crumb={_yf_crumb}"
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with _yf_opener.open(req, timeout=10) as resp:
            data = json.loads(resp.read())
        qr = data.get("quoteSummary", {}).get("result", [])
        if not qr:
            return result

        ap = qr[0].get("assetProfile", {})
        ks = qr[0].get("defaultKeyStatistics", {})
        fd = qr[0].get("financialData", {})
        sd = qr[0].get("summaryDetail", {})
        ih = qr[0].get("insiderHoldings", {})

        # Business summary
        desc = ap.get("longBusinessSummary", "")
        if desc and len(desc) > 50:
            result["summary"] = desc

        # KPIs - helper to extract raw value from {"raw": value} dicts
        def _val(obj):
            if isinstance(obj, dict):
                return obj.get("raw")
            return obj

        kpis = result["kpis"]
        kpis["sector"] = ap.get("sector", "")
        kpis["industry"] = ap.get("industry", "")

        # From assetProfile
        emp = ap.get("fullTimeEmployees")
        if emp is not None:
            kpis["employees"] = _val(emp)

        # From defaultKeyStatistics
        _ks_map = {
            "trailingPE": "pe",
            "forwardPE": "forwardPE",
            "trailingEps": "eps",
            "beta": "beta",
            "priceToBook": "priceToBook",
            "priceToSalesTrailing12Months": "priceToSales",
            "fiftyTwoWeekHigh": "high52w",
            "fiftyTwoWeekLow": "low52w",
            "fiftyDayAverage": "fiftyDayAvg",
            "twoHundredDayAverage": "twoHundredDayAvg",
            "sharesOutstanding": "sharesOut",
            "floatShares": "floatShares",
            "sharesShort": "sharesShort",
            "sharesShortPriorMonth": "sharesShortPriorMonth",
            "shortRatio": "shortRatio",
            "shortPercentOfFloat": "shortPctFloat",
            "enterpriseValue": "enterpriseValue",
            "dividendRate": "dividendRate",
        }
        for ks_key, kpi_key in _ks_map.items():
            v = _val(ks.get(ks_key))
            if v is not None and isinstance(v, (int, float)):
                kpis[kpi_key] = v

        # From financialData
        _fd_map = {
            "totalRevenue": "revenue",
            "revenuePerShare": "revenuePerShare",
            "profitMargins": "profitMargin",
            "returnOnAssets": "roa",
            "returnOnEquity": "roe",
            "freeCashflow": "freeCashflow",
            "operatingMargins": "operatingMargin",
            "earningsGrowth": "earningsGrowth",
            "revenueGrowth": "revenueGrowth",
            "targetMeanPrice": "analystTarget",
            "numberOfAnalysts": "analystCount",
            "recommendationKey": "recommendation",
        }
        for fd_key, kpi_key in _fd_map.items():
            v = _val(fd.get(fd_key))
            if v is not None and isinstance(v, (int, float, str)):
                kpis[kpi_key] = v

        # From summaryDetail
        if _val(sd.get("yield")):
            kpis["divYield"] = _val(sd.get("yield"))
        if _val(sd.get("beta")):
            kpis["beta"] = _val(sd.get("beta"))

        # From insiderHoldings
        ih_holders = ih.get("insiders", [])
        if ih_holders:
            total_insider = sum(
                _val(x.get("positionDirectPercentage", 0)) for x in ih_holders
            )
            if total_insider:
                kpis["insiderPct"] = total_insider

        # Remove empty strings, None values
        result["kpis"] = {k: v for k, v in kpis.items() if v not in ("", None)}
        log.info(
            "Yahoo KPI OK for %s: summary=%d, kpis=%s",
            ticker,
            len(result["summary"]),
            list(result["kpis"].keys()),
        )
    except Exception as e:
        log.info("Yahoo KPI error for %s: %s", ticker, e)
    return result


def _load_cik_map():
    """Load SEC ticker -> CIK mapping (cached, one-time)."""
    global _cik_map_loaded
    if _cik_map_loaded:
        return
    try:
        req = urllib.request.Request(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": "MomentumScreener/1.0 (contact@example.com)"},
        )
        resp = urllib.request.urlopen(req, timeout=10)
        raw = resp.read()
        if resp.headers.get("Content-Encoding") == "gzip":
            import gzip

            raw = gzip.decompress(raw)
        data = json.loads(raw)
        for v in data.values():
            _cik_map[v["ticker"].upper()] = str(v["cik_str"])
    except Exception as e:
        log.warning(f"Failed to load SEC CIK map: {e}")
    _cik_map_loaded = True


def _check_shelf_sync(ticker):
    """Check SEC EDGAR for active S-3 shelf registration filings."""
    if ticker in _shelf_cache:
        return _shelf_cache[ticker]
    _load_cik_map()
    cik = _cik_map.get(ticker)
    if not cik:
        _shelf_cache[ticker] = {"hasShelf": False}
        return _shelf_cache[ticker]
    padded = cik.zfill(10)
    try:
        req = urllib.request.Request(
            f"https://data.sec.gov/submissions/CIK{padded}.json",
            headers={"User-Agent": "MomentumScreener/1.0 (contact@example.com)"},
        )
        resp = urllib.request.urlopen(req, timeout=8)
        raw = resp.read()
        if resp.headers.get("Content-Encoding") == "gzip":
            import gzip

            raw = gzip.decompress(raw)
        data = json.loads(raw)
        # Grab SEC company metadata
        sic_desc = data.get("sicDescription", "")
        state = data.get("stateOfIncorporation", "")
        website = data.get("website", "")
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        cutoff = (datetime.utcnow() - timedelta(days=3 * 365)).strftime("%Y-%m-%d")
        shelf_info = {"hasShelf": False}
        for i, form in enumerate(forms):
            if form in ("S-3", "S-3/A", "S-3ASR"):
                filing_date = dates[i] if i < len(dates) else ""
                if filing_date >= cutoff:
                    shelf_info = {
                        "hasShelf": True,
                        "filingDate": filing_date,
                        "form": form,
                    }
                    # Build direct EDGAR filing URL
                    if i < len(accessions) and i < len(primary_docs):
                        acc = accessions[i].replace("-", "")
                        shelf_info["url"] = (
                            f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{primary_docs[i]}"
                        )
                    break
        if sic_desc:
            shelf_info["sicDescription"] = sic_desc
        if state:
            shelf_info["state"] = state
        if website:
            shelf_info["website"] = website
        _shelf_cache[ticker] = shelf_info
        return shelf_info
    except Exception:
        pass
    _shelf_cache[ticker] = {"hasShelf": False}
    return _shelf_cache[ticker]


async def _fetch_contract_info(ticker):
    """Get company details + live quote + summary for a ticker."""
    # Check cache for static info
    if ticker not in _contract_info_cache:
        c = Stock(ticker, "SMART", "USD")
        details = await ib.reqContractDetailsAsync(c)
        if not details:
            return None
        d = details[0]
        cc = d.contract
        _contract_info_cache[ticker] = {
            "conid": cc.conId,
            "symbol": cc.symbol,
            "companyName": d.longName or "",
            "industry": d.industry or "",
            "category": d.category or "",
            "subcategory": d.subcategory or "",
            "exchange": cc.primaryExchange or cc.exchange or "",
        }
        _news_conid_cache[ticker] = cc.conId
    info = dict(_contract_info_cache[ticker])

    # Fetch live quote snapshot
    conid = info["conid"]
    c = Contract(conId=conid)
    try:
        tickers = await ib.reqTickersAsync(c)
        if tickers:
            t = tickers[0]
            mp = t.marketPrice()
            price = mp if _is_num(mp) else (t.last if _is_num(t.last) else None)
            if price is not None:
                info["price"] = round(price, 2)
            if _is_num(t.close) and t.close > 0 and price is not None:
                info["change"] = round(price - t.close, 2)
                info["changePct"] = round((price - t.close) / t.close * 100, 2)
                info["prevClose"] = round(t.close, 2)
            if _is_num(t.high):
                info["high"] = round(t.high, 2)
            if _is_num(t.low):
                info["low"] = round(t.low, 2)
            if _is_num(t.open):
                info["open"] = round(t.open, 2)
            if _is_num(t.volume):
                info["volume"] = int(t.volume)
    except Exception as e:
        log.warning(f"Snapshot failed for {ticker}: {e}")

    return info


def _fetch_yfinance_contract_info(ticker):
    """Blocking fallback: get contract info from Yahoo API via urllib."""
    if not _yf_ensure_crumb():
        return None

    _UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    try:
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=assetProfile,price&crumb={_yf_crumb}"
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with _yf_opener.open(req, timeout=10) as resp:
            data = json.loads(resp.read())
        qr = data.get("quoteSummary", {}).get("result", [])
        if not qr:
            return None

        ap = qr[0].get("assetProfile", {})
        pr = qr[0].get("price", {})

        # Company name from price module
        company_name = pr.get("longName") or pr.get("shortName") or ticker
        if not company_name or company_name == ticker:
            # If no name, return None
            if not ap.get("sector"):
                return None

        info = {
            "symbol": ticker,
            "companyName": company_name,
            "industry": ap.get("industry") or "",
            "sector": ap.get("sector") or "",
            "category": "",
            "subcategory": "",
            "exchange": pr.get("exchange") or pr.get("exchangeName") or "",
        }

        # Price data from price module
        current = pr.get("regularMarketPrice")
        if isinstance(current, dict):
            current = current.get("raw")
        if current is not None and isinstance(current, (int, float)):
            info["price"] = round(current, 2)

        log.info("Yahoo contract-info OK for %s: %s", ticker, info.get("companyName"))
        return info
    except Exception as e:
        log.warning("Yahoo contract-info error for %s: %s", ticker, e)
        return None


async def h_contract_info(req):
    ticker = req.match_info.get("ticker", "").upper().strip()
    if not re.match(r"^[A-Z0-9.]{1,10}$", ticker):
        return web.json_response({"error": "invalid ticker"}, status=400)

    info = None
    # Try IB first
    if ib_connected:
        try:
            loop = asyncio.get_event_loop()
            info_task = _await_ib(_fetch_contract_info(ticker), timeout=12)
            shelf_task = loop.run_in_executor(None, _check_shelf_sync, ticker)
            info, shelf = await asyncio.gather(
                info_task, shelf_task, return_exceptions=True
            )
            if isinstance(info, Exception):
                info = None
            if isinstance(shelf, dict) and info:
                info["shelf"] = shelf
        except Exception as e:
            log.warning("IB contract-info failed for %s: %s", ticker, e)

    # Fallback to urllib if IB didn't provide data
    if not info:
        try:
            loop = asyncio.get_event_loop()
            yf_info = await asyncio.wait_for(
                loop.run_in_executor(None, _fetch_yfinance_contract_info, ticker),
                timeout=15,
            )
            if yf_info:
                info = yf_info
        except Exception as e:
            log.warning("urllib fallback failed for %s: %s", ticker, e)

    if not info:
        return web.json_response({"error": "not found"}, status=404)
    return web.json_response(info)


async def h_contract_kpis(req):
    """Fetch KPIs + summary (IB fundamental data / urllib fallback)."""
    ticker = req.match_info.get("ticker", "").upper().strip()
    if not re.match(r"^[A-Z0-9.]{1,10}$", ticker):
        return web.json_response({"error": "invalid ticker"}, status=400)

    result = {}
    # Try IB first if connected
    if ib_connected:
        try:
            ydata = await _fetch_ib_kpis(ticker)
            if isinstance(ydata, dict):
                if ydata.get("summary"):
                    result["summary"] = ydata["summary"]
                if ydata.get("kpis"):
                    result["kpis"] = ydata["kpis"]
        except Exception as e:
            log.warning("IB KPI fetch failed for %s: %s", ticker, e)

    # Fallback to urllib directly if IB didn't provide data
    if not result.get("summary") and not result.get("kpis"):
        try:
            loop = asyncio.get_event_loop()
            yf_data = await asyncio.wait_for(
                loop.run_in_executor(None, _fetch_yahoo_stdlib, ticker),
                timeout=15,
            )
            if isinstance(yf_data, dict):
                if yf_data.get("summary"):
                    result["summary"] = yf_data["summary"]
                if yf_data.get("kpis"):
                    result["kpis"] = yf_data["kpis"]
        except Exception as e:
            log.warning("urllib KPI fallback failed for %s: %s", ticker, e)

    return web.json_response(result)


async def _get_news_providers():
    """Fetch and cache available news provider codes."""
    global _news_providers
    if _news_providers:
        return _news_providers
    try:
        providers = await ib.reqNewsProvidersAsync()
        if providers:
            _news_providers = "+".join(p.code for p in providers)
            log.info(f"News providers: {_news_providers}")
    except Exception as e:
        log.warning(f"reqNewsProviders failed: {e}")
    if not _news_providers:
        _news_providers = "BRFG+BRFUPDN+DJNL"  # free defaults
    return _news_providers


async def _resolve_news_conid(ticker):
    """Get conId for a ticker symbol (cached)."""
    if ticker in _news_conid_cache:
        return _news_conid_cache[ticker]
    c = Stock(ticker, "SMART", "USD")
    details = await ib.reqContractDetailsAsync(c)
    if details:
        conid = details[0].contract.conId
        _news_conid_cache[ticker] = conid
        return conid
    return None


async def _fetch_tws_news(ticker):
    """Fetch historical news headlines from TWS for a ticker."""
    conid = await _resolve_news_conid(ticker)
    if not conid:
        return []
    providers = await _get_news_providers()
    end = datetime.utcnow()
    start = end - timedelta(days=2)
    try:
        headlines = await ib.reqHistoricalNewsAsync(
            conid,
            providers,
            start.strftime("%Y-%m-%d %H:%M:%S"),
            end.strftime("%Y-%m-%d %H:%M:%S"),
            50,
        )
    except Exception as e:
        log.warning(f"reqHistoricalNews failed for {ticker}: {e}")
        return []
    if not headlines:
        return []
    items = []
    for h in headlines:
        title = h.headline or ""
        # Clean provider tags like {RTRS} from headline
        title = re.sub(r"\{[^}]*\}", "", title).strip()
        items.append(
            {
                "title": title,
                "published": (
                    (
                        h.time.replace(tzinfo=timezone.utc).isoformat()
                        if h.time and not h.time.tzinfo
                        else h.time.isoformat()
                    )
                    if h.time
                    else ""
                ),
                "source": h.providerCode,
                "articleId": h.articleId,
                "link": "",  # TWS news has no web link; article body via reqNewsArticle
            }
        )
    items.sort(key=lambda x: x["published"], reverse=True)
    return items


import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime


async def _fetch_google_news(ticker: str) -> list:
    """Fetch recent news from Google News RSS for a stock ticker."""
    url = (
        f"https://news.google.com/rss/search?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en"
    )
    loop = asyncio.get_event_loop()
    try:

        def _do_fetch():
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=6) as resp:
                return resp.read()

        data = await loop.run_in_executor(None, _do_fetch)
        root = ET.fromstring(data)
        items = []
        for item in root.iter("item"):
            title_el = item.find("title")
            link_el = item.find("link")
            pub_el = item.find("pubDate")
            source_el = item.find("source")
            title = title_el.text if title_el is not None else ""
            link = link_el.text if link_el is not None else ""
            source = source_el.text if source_el is not None else "Google News"
            published = ""
            if pub_el is not None and pub_el.text:
                try:
                    published = parsedate_to_datetime(pub_el.text).isoformat()
                except Exception:
                    published = pub_el.text
            items.append(
                {
                    "title": title,
                    "published": published,
                    "source": source,
                    "articleId": "",
                    "link": link,
                }
            )
        return items[:20]
    except Exception as e:
        log.debug(f"Google News RSS fetch failed for {ticker}: {e}")
        return []


async def h_news(req):
    ticker = req.match_info.get("ticker", "").upper().strip()
    if not re.match(r"^[A-Z0-9.]{1,10}$", ticker):
        return web.json_response([], status=400)
    # Fetch TWS news and Google News in parallel
    tws_task = None
    if ib_connected:
        tws_task = asyncio.ensure_future(_await_ib(_fetch_tws_news(ticker), timeout=10))
    google_task = asyncio.ensure_future(_fetch_google_news(ticker))
    tws_items = []
    if tws_task:
        try:
            tws_items = await tws_task
        except Exception as e:
            log.warning(f"TWS news failed for {ticker}: {e}")
    google_items = await google_task
    # Merge: TWS first, then Google, deduplicate by title similarity
    seen_titles = set()
    merged = []
    for item in tws_items:
        key = item["title"].lower().strip()[:60]
        if key not in seen_titles:
            seen_titles.add(key)
            merged.append(item)
    for item in google_items:
        key = item["title"].lower().strip()[:60]
        if key not in seen_titles:
            seen_titles.add(key)
            merged.append(item)
    # Sort by published date descending
    merged.sort(key=lambda x: x.get("published", ""), reverse=True)
    return web.json_response(merged[:30])


async def _fetch_news_article(provider, article_id):
    """Fetch article body — runs on IB loop."""
    return await ib.reqNewsArticleAsync(provider, article_id, [])


async def h_news_article(req):
    """Fetch full article body: GET /news/article/{provider}/{articleId}"""
    provider = req.match_info.get("provider", "").strip()
    article_id = req.match_info.get("articleId", "").strip()
    if not provider or not article_id:
        return web.json_response({"error": "missing params"}, status=400)
    if not ib_connected:
        return web.json_response({"error": "not connected"}, status=503)
    try:
        article = await _await_ib(_fetch_news_article(provider, article_id), timeout=10)
        if article:
            return web.json_response(
                {"type": article.articleType, "text": article.articleText}
            )
        return web.json_response({"error": "not found"}, status=404)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


async def h_gw(req):
    return web.json_response(
        {
            "ws": f"ws://localhost:{PORT}/v1/api/ws",
            "gwHost": TWS_HOST,
            "gwPort": TWS_PORT,
            "mode": "tws",
        }
    )


# ══════════════════════════════════════════════════════════════
#  WEBSOCKET HANDLER
# ══════════════════════════════════════════════════════════════


async def h_ws(req):
    ws = web.WebSocketResponse()
    await ws.prepare(req)
    _ws_clients.add(ws)
    log.info(f"WS client connected ({len(_ws_clients)} total)")

    # Immediately send sts — frontend gates all subscriptions on this
    await ws.send_str(
        json.dumps({"topic": "sts", "args": {"authenticated": ib_connected}})
    )

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                d = msg.data.strip()
                if d == "tic":
                    continue

                # smd+{conid}+{json}
                if d.startswith("smd+"):
                    parts = d.split("+", 2)
                    if len(parts) >= 2:
                        conid = parts[1]
                        _md_subs.setdefault(conid, set()).add(ws)
                        # Clear halt latch so new client gets current halt state on next tick
                        _halt_state.pop(conid, None)
                        _sched(_sub_md(conid))
                        # If conid already has a market data error, send it immediately
                        if conid in _mderr_conids:
                            try:
                                await ws.send_str(
                                    json.dumps(
                                        {
                                            "topic": "mderr",
                                            "conid": conid,
                                            "code": 354,
                                            "msg": _mderr_conids[conid],
                                        }
                                    )
                                )
                            except Exception:
                                pass
                    continue

                # umd+{conid}+{}
                if d.startswith("umd+"):
                    parts = d.split("+", 2)
                    if len(parts) >= 2:
                        conid = parts[1]
                        s = _md_subs.get(conid, set())
                        s.discard(ws)
                        if not s:
                            _sched(_unsub_md(conid))
                    continue

                # sbd+{accountId}+{conid}
                if d.startswith("sbd+"):
                    parts = d.split("+", 3)
                    if len(parts) >= 3:
                        conid = parts[2]
                        _depth_subs.setdefault(conid, set()).add(ws)
                        _sched(_sub_depth(conid))
                        # Send last-known book snapshot immediately
                        is_halted = _halt_state.get(conid) is True
                        t = _conid_to_depth_ticker.get(conid)
                        if is_halted and conid in _depth_cache:
                            # During halt, send cached depth with frozen flag
                            try:
                                snap = json.dumps(
                                    {
                                        "topic": f"sbd+{ib_account}+{conid}",
                                        "data": _depth_cache[conid],
                                        "_frozen": True,
                                    }
                                )
                                _send_to_ws(ws, snap)
                            except Exception:
                                pass
                        elif t:
                            # Only send snapshot if data is fresh (< 2s) to avoid stale flash
                            last_upd = _depth_last_real_update.get(conid, 0)
                            if time.monotonic() - last_upd < 2.0:
                                try:
                                    seq = _depth_seq.get(conid, 0) + 1
                                    _depth_seq[conid] = seq
                                    snap = _build_depth_msg(t, conid, seq)
                                    _send_to_ws(ws, snap)
                                except Exception:
                                    pass
                    continue

                # ubd+{accountId}+{conid}
                if d.startswith("ubd+"):
                    parts = d.split("+", 3)
                    if len(parts) >= 3:
                        conid = parts[2]
                        s = _depth_subs.get(conid, set())
                        s.discard(ws)
                        if not s:
                            _depth_subs.pop(conid, None)
                            _sched(_unsub_depth(conid))
                    continue

            elif msg.type == web.WSMsgType.ERROR:
                log.error(f"WS error: {ws.exception()}")
    finally:
        _ws_clients.discard(ws)
        for conid, clients in list(_md_subs.items()):
            clients.discard(ws)
            if not clients:
                del _md_subs[conid]
                _sched(_unsub_md(conid))
        for conid, clients in list(_depth_subs.items()):
            clients.discard(ws)
            if not clients:
                del _depth_subs[conid]
                _sched(_unsub_depth(conid))
        log.info(f"WS client disconnected ({len(_ws_clients)} total)")
    return ws


# ══════════════════════════════════════════════════════════════
#  APP SETUP
# ══════════════════════════════════════════════════════════════

# When running from a PyInstaller bundle, __file__ is inside the temp dir.
# sys._MEIPASS points to the extracted bundle root.
if getattr(sys, "_MEIPASS", None):
    STATIC_DIR = Path(sys._MEIPASS)
else:
    STATIC_DIR = Path(__file__).parent


async def h_index(req):
    """Serve index.html for root path."""
    return web.FileResponse(STATIC_DIR / "index.html")


async def h_analytics(req):
    """Serve analytics.html for /analytics path."""
    resp = web.FileResponse(STATIC_DIR / "analytics.html")
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


async def h_release_notes_page(req):
    """Serve release-notes.html page."""
    return web.FileResponse(STATIC_DIR / "release-notes.html")


# ── Cognito Email-OTP Auth ───────────────────────────────────


def _check_subscription(id_token):
    """Decode JWT and check cognito:groups for an active plan. Returns (ok, err_msg)."""
    import base64 as _b64

    try:
        payload_b64 = id_token.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        claims = json.loads(_b64.urlsafe_b64decode(payload_b64))
    except Exception:
        return False, "Unable to decode token."
    groups = claims.get("cognito:groups", [])
    valid = {"scanner", "scanner-trading", "scanner-trading-analytics"}
    if any(g in valid for g in groups):
        return True, ""
    return False, "No active subscription found. Please contact support."


def _cognito_post(action, payload):
    """Call Cognito IDP API directly (no SDK needed)."""
    url = f"https://cognito-idp.{COGNITO_REGION}.amazonaws.com/"
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": f"AWSCognitoIdentityProviderService.{action}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read()), None
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        try:
            err = json.loads(err_body)
        except Exception:
            err = {"__type": "Unknown", "message": err_body}
        return None, err


async def h_auth_config(req):
    """Tell the frontend whether auth is required (Cognito configured)."""
    return web.json_response(
        {
            "authRequired": bool(COGNITO_USER_POOL_ID and COGNITO_CLIENT_ID),
        }
    )


async def h_auth_login(req):
    """Start CUSTOM_AUTH flow → Cognito sends OTP to email."""
    if not COGNITO_USER_POOL_ID or not COGNITO_CLIENT_ID:
        return web.json_response({"error": "Auth not configured"}, status=503)
    try:
        body = await req.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    email = (body.get("email") or "").strip().lower()
    if not email or "@" not in email:
        return web.json_response({"error": "Valid email required"}, status=400)

    result, err = _cognito_post(
        "InitiateAuth",
        {
            "AuthFlow": "CUSTOM_AUTH",
            "ClientId": COGNITO_CLIENT_ID,
            "AuthParameters": {"USERNAME": email},
        },
    )
    if err:
        etype = err.get("__type", "")
        msg = err.get("message", "Auth failed")
        if "NotAuthorizedException" in etype:
            return web.json_response(
                {"error": "Account not activated. Please wait for approval."},
                status=403,
            )
        if "UserNotFoundException" in etype:
            return web.json_response(
                {"error": "No account found for this email."}, status=404
            )
        log.warning(f"Cognito InitiateAuth error: {etype}: {msg}")
        return web.json_response({"error": msg}, status=400)

    return web.json_response(
        {
            "session": result.get("Session", ""),
            "challengeName": result.get("ChallengeName", ""),
        }
    )


async def h_auth_verify(req):
    """Verify OTP code → returns tokens."""
    if not COGNITO_CLIENT_ID:
        return web.json_response({"error": "Auth not configured"}, status=503)
    try:
        body = await req.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    email = (body.get("email") or "").strip().lower()
    code = (body.get("code") or "").strip()
    session = body.get("session", "")
    if not email or not code or not session:
        return web.json_response(
            {"error": "email, code, and session required"}, status=400
        )

    result, err = _cognito_post(
        "RespondToAuthChallenge",
        {
            "ClientId": COGNITO_CLIENT_ID,
            "ChallengeName": "CUSTOM_CHALLENGE",
            "Session": session,
            "ChallengeResponses": {
                "USERNAME": email,
                "ANSWER": code,
            },
        },
    )
    if err:
        etype = err.get("__type", "")
        msg = err.get("message", "Verification failed")
        if "NotAuthorizedException" in etype or "CodeMismatchException" in etype:
            return web.json_response(
                {"error": "Invalid or expired code. Please try again."}, status=401
            )
        log.warning(f"Cognito RespondToAuthChallenge error: {etype}: {msg}")
        return web.json_response({"error": msg}, status=400)

    # May be another challenge round or final tokens
    if result.get("ChallengeName"):
        return web.json_response(
            {
                "session": result.get("Session", ""),
                "challengeName": result.get("ChallengeName", ""),
            }
        )

    auth = result.get("AuthenticationResult", {})
    id_token = auth.get("IdToken", "")
    if id_token:
        ok, err_msg = _check_subscription(id_token)
        if not ok:
            return web.json_response({"error": err_msg}, status=403)
    return web.json_response(
        {
            "idToken": id_token,
            "accessToken": auth.get("AccessToken", ""),
            "refreshToken": auth.get("RefreshToken", ""),
            "expiresIn": auth.get("ExpiresIn", 3600),
        }
    )


async def h_auth_refresh(req):
    """Refresh tokens using refresh token (or secure Windows desktop session token)."""
    if not COGNITO_CLIENT_ID:
        return web.json_response({"error": "Auth not configured"}, status=503)
    try:
        body = await req.json()
    except Exception:
        body = {}
    refresh_token = (body or {}).get("refreshToken", "")
    if not refresh_token and _IS_WINDOWS:
        refresh_token = _load_refresh_token_secure()
    if not refresh_token:
        return web.json_response({"error": "refreshToken required"}, status=400)

    result, err = _cognito_post(
        "InitiateAuth",
        {
            "AuthFlow": "REFRESH_TOKEN_AUTH",
            "ClientId": COGNITO_CLIENT_ID,
            "AuthParameters": {"REFRESH_TOKEN": refresh_token},
        },
    )
    if err:
        etype = err.get("__type", "")
        msg = err.get("message", "Refresh failed")
        log.warning(f"Cognito refresh error: {etype}: {msg}")
        return web.json_response({"error": msg}, status=401)

    auth = result.get("AuthenticationResult", {})
    id_token = auth.get("IdToken", "")
    if id_token:
        ok, err_msg = _check_subscription(id_token)
        if not ok:
            return web.json_response({"error": err_msg}, status=403)
    return web.json_response(
        {
            "idToken": id_token,
            "accessToken": auth.get("AccessToken", ""),
            "expiresIn": auth.get("ExpiresIn", 3600),
        }
    )


async def h_auth_session_store(req):
    """Windows desktop only: store refresh token with DPAPI."""
    if not _IS_WINDOWS:
        return web.json_response(
            {"ok": False, "error": "Secure token storage is Windows-only"}, status=400
        )
    try:
        body = await req.json()
    except Exception:
        return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

    refresh_token = (body or {}).get("refreshToken", "")
    if not refresh_token:
        return web.json_response(
            {"ok": False, "error": "refreshToken required"}, status=400
        )

    ok = _store_refresh_token_secure(refresh_token)
    return web.json_response({"ok": bool(ok), "platform": sys.platform})


async def h_auth_session_status(req):
    """Check whether a secure refresh token exists for desktop session resume."""
    has_refresh = bool(_load_refresh_token_secure()) if _IS_WINDOWS else False
    return web.json_response(
        {
            "ok": True,
            "platform": sys.platform,
            "windowsSecureStorage": _IS_WINDOWS,
            "hasRefreshToken": has_refresh,
        }
    )


async def h_auth_session_clear(req):
    """Clear secure desktop refresh token on logout."""
    _clear_refresh_token_secure()
    return web.json_response({"ok": True, "platform": sys.platform})


async def h_auth_me(req):
    """Decode the ID token and return user info (no server-side validation needed — local app)."""
    try:
        body = await req.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    id_token = body.get("idToken", "")
    if not id_token:
        return web.json_response({"error": "idToken required"}, status=400)
    # Decode JWT payload (base64url, no signature verification — local trusted server)
    import base64

    try:
        payload_b64 = id_token.split(".")[1]
        # Add padding
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
    except Exception:
        return web.json_response({"error": "Invalid token"}, status=400)
    return web.json_response(
        {
            "email": payload.get("email", ""),
            "groups": payload.get("cognito:groups", []),
            "sub": payload.get("sub", ""),
        }
    )


async def h_connection_mode(req):
    """Set preferred connection mode and optionally force immediate reconnect."""
    global _connection_mode_preference, TWS_PORT, _manual_disconnect_in_progress
    try:
        body = await req.json()
    except Exception:
        body = {}

    mode = str((body or {}).get("mode", "")).strip().lower()
    if mode not in {"gateway", "tws"}:
        return web.json_response(
            {"ok": False, "error": "mode must be 'gateway' or 'tws'"}, status=400
        )

    reconnect = bool((body or {}).get("reconnect", True))
    _connection_mode_preference = mode
    TWS_PORT = None  # force autodetect using preferred mode order on next connect

    if reconnect and ib_loop and ib_loop.is_running():

        async def _reconnect_for_mode():
            global _manual_disconnect_in_progress
            try:
                if ib and ib.isConnected():
                    _manual_disconnect_in_progress = True
                    ib.disconnect()
                    await asyncio.sleep(0.2)
                await _ib_connect()
            except Exception as e:
                log.warning(f"manual reconnect for mode switch failed: {e}")

        ib_loop.call_soon_threadsafe(
            lambda: asyncio.ensure_future(_reconnect_for_mode(), loop=ib_loop)
        )

    return web.json_response(
        {
            "ok": True,
            "mode": _connection_mode_preference,
            "reconnectRequested": reconnect,
            "connected": ib_connected,
            "twsPort": TWS_PORT,
            "candidates": _candidate_ports_for_mode(),
        }
    )


def create_app():
    @web.middleware
    async def _activity_middleware(request, handler):
        global _http_req_count
        p = request.path or ""
        if (
            p.startswith("/v1/api/")
            or p.startswith("/api/")
            or p.startswith("/auth/")
            or p == "/gw"
        ):
            _http_req_count += 1
        return await handler(request)

    app = web.Application(middlewares=[_activity_middleware])

    async def _depth_heartbeat(app):
        """Detect stale or missing depth subscriptions and refresh from TWS."""
        while True:
            await asyncio.sleep(2)
            now = time.monotonic()
            for conid, clients in list(_depth_subs.items()):
                if not clients:
                    continue
                t = _conid_to_depth_ticker.get(conid)
                is_halted = _halt_state.get(conid) is True
                # Recover missing depth ticker while clients are still subscribed.
                if not is_halted and not t:
                    log.info(
                        f"Depth missing for {conid} with active clients — subscribing"
                    )
                    try:
                        await _await_ib(_sub_depth(conid, force=True), timeout=10)
                        _depth_last_real_update[conid] = now
                    except Exception as e:
                        log.warning(f"Depth subscribe recovery failed for {conid}: {e}")
                    continue
                # Detect stale depth: no real TWS callback in _DEPTH_STALE_SECS
                if not is_halted and t:
                    last = _depth_last_real_update.get(conid, 0)
                    if last and (now - last) > _DEPTH_STALE_SECS:
                        log.info(
                            f"Depth stale for {conid} ({now - last:.0f}s) — re-subscribing"
                        )
                        try:
                            # IMPORTANT: depth subscriptions must run on IB loop.
                            await _await_ib(_sub_depth(conid, force=True), timeout=10)
                            _depth_last_real_update[conid] = (
                                now  # reset to avoid rapid re-sub
                            )
                        except Exception as e:
                            log.warning(f"Depth re-sub failed for {conid}: {e}")

    async def _start_depth_heartbeat(app):
        app["_depth_hb"] = asyncio.ensure_future(_depth_heartbeat(app))

    async def _stop_depth_heartbeat(app):
        app["_depth_hb"].cancel()
        try:
            await app["_depth_hb"]
        except asyncio.CancelledError:
            pass

    app.on_startup.append(_start_depth_heartbeat)
    app.on_cleanup.append(_stop_depth_heartbeat)

    # Load trade journal from disk
    _load_journal()

    # REST (same paths the frontend uses)
    app.router.add_get("/v1/api/iserver/auth/status", h_auth_status)
    app.router.add_get("/api/health", h_health)
    app.router.add_post("/v1/api/connection/mode", h_connection_mode)
    app.router.add_post("/v1/api/iserver/scanner/run", h_scanner_run)
    app.router.add_get("/v1/api/iserver/marketdata/snapshot", h_snapshot)
    app.router.add_get("/v1/api/iserver/marketdata/depth", h_depth_snapshot)
    app.router.add_get("/v1/api/debug/depth-probe", h_depth_probe)
    app.router.add_get("/v1/api/iserver/marketdata/history", h_history)
    app.router.add_post("/v1/api/iserver/secdef/search", h_search)
    app.router.add_get("/v1/api/portfolio/accounts", h_accounts)
    app.router.add_get("/v1/api/portfolio/{accountId}/positions/{page}", h_positions)
    app.router.add_get("/v1/api/portfolio/{accountId}/summary", h_summary)
    app.router.add_get("/v1/api/portfolio/{accountId}/ledger", h_ledger)
    app.router.add_get("/v1/api/iserver/account/pnl/partitioned", h_pnl)
    app.router.add_get("/v1/api/iserver/account/orders", h_orders)
    app.router.add_post("/v1/api/iserver/account/{accountId}/orders", h_place_order)
    app.router.add_post("/v1/api/iserver/reply/{replyId}", h_reply)
    app.router.add_delete(
        "/v1/api/iserver/account/{accountId}/order/{orderId}", h_cancel_order
    )
    app.router.add_put(
        "/v1/api/iserver/account/{accountId}/order/{orderId}", h_modify_order
    )

    # WebSocket
    app.router.add_get("/v1/api/ws", h_ws)

    # External proxies
    app.router.add_get("/gw", h_gw)
    app.router.add_get("/float/{ticker}", h_float)
    app.router.add_post("/floats", h_floats_batch)
    app.router.add_post("/check-tradable", h_check_tradable)
    app.router.add_get("/news/article/{provider}/{articleId}", h_news_article)
    app.router.add_get("/news/{ticker}", h_news)
    app.router.add_get("/contract-info/{ticker}", h_contract_info)
    app.router.add_get("/contract-kpis/{ticker}", h_contract_kpis)
    app.router.add_get("/splits/{ticker}", h_splits)

    # Trade journal
    app.router.add_get("/api/trades", h_trades_journal)
    app.router.add_post("/api/trades/import", h_trades_import_csv)
    app.router.add_delete("/api/trades", h_trades_clear)

    # User settings persistence
    app.router.add_get("/api/settings", h_settings_get)
    app.router.add_post("/api/settings", h_settings_save)
    app.router.add_get("/api/release-notes/latest", h_release_notes_latest)
    app.router.add_get("/api/release-notes/all", h_release_notes_all)

    # Auth (Cognito email OTP)
    app.router.add_get("/auth/config", h_auth_config)
    app.router.add_post("/auth/login", h_auth_login)
    app.router.add_post("/auth/verify", h_auth_verify)
    app.router.add_post("/auth/refresh", h_auth_refresh)
    app.router.add_post("/auth/me", h_auth_me)
    app.router.add_post("/auth/session/store", h_auth_session_store)
    app.router.add_get("/auth/session/status", h_auth_session_status)
    app.router.add_post("/auth/session/clear", h_auth_session_clear)
    app.router.add_post("/api/connection/mode", h_connection_mode)

    # Root serves index.html
    app.router.add_get("/", h_index)
    app.router.add_get("/analytics", h_analytics)
    app.router.add_get("/release-notes", h_release_notes_page)
    app.router.add_get("/release-notes.html", h_release_notes_page)

    # Static files (index.html etc.) — MUST be last
    app.router.add_static("/", STATIC_DIR, show_index=False)

    return app


def main():
    global _aio_loop
    # Ensure this thread has an event loop (needed when launched from a
    # background thread in the exe launcher — Python 3.10+ no longer
    # auto-creates a loop for non-main threads).
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    log.info(f"TWS Bridge starting on http://localhost:{PORT}")
    if TWS_PORT:
        log.info(f"Connecting to TWS at {TWS_HOST}:{TWS_PORT} (clientId={CLIENT_ID})")
    else:
        log.info(
            f"Auto-detecting TWS/Gateway port from {_candidate_ports_for_mode()} (clientId={CLIENT_ID})"
        )

    start_ib()
    time.sleep(2)  # let IB connect

    # Start watchdog thread for IB reconnection safety net
    threading.Thread(target=_ib_watchdog, daemon=True).start()

    app = create_app()
    _aio_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_aio_loop)
    try:
        web.run_app(
            app,
            host="0.0.0.0",
            port=PORT,
            loop=_aio_loop,
            print=lambda msg: log.info(msg),
        )
    finally:
        log.info("Shutting down — disconnecting IB...")
        try:
            if ib_loop and ib_loop.is_running():
                ib_loop.call_soon_threadsafe(ib_loop.stop)
            if ib.isConnected():
                ib.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception:
        log.critical("FATAL — unhandled exception:\n%s", traceback.format_exc())
        sys.exit(1)
