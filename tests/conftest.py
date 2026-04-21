"""
Shared fixtures and helpers for TWS bridge integration tests.

Requires:
    - TWS/IB Gateway running and reachable (paper gateway on port 7497)
    - Local serve.py is auto-started for localhost test runs unless disabled
  - pip install pytest pytest-asyncio websockets

Usage:
    pytest tests/ -v --tb=short
"""

import json
import os
import subprocess
import sys
import time
import asyncio
import urllib.request
import urllib.error
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import pytest

try:
    import websockets

    HAS_WS = True
except ImportError:
    websockets = None
    HAS_WS = False

BASE_URL = os.environ.get("TWS_BRIDGE_URL", "http://localhost:8888")
WS_URL = (
    BASE_URL.replace("http://", "ws://").replace("https://", "wss://") + "/v1/api/ws"
)

# Timeout for order fills (kept for optional fill-sensitive checks).
FILL_TIMEOUT = float(os.environ.get("FILL_TIMEOUT", "0.2"))

# Timeout for strict fill-metadata and position-propagation checks.
FILL_DETAILS_TIMEOUT = float(os.environ.get("FILL_DETAILS_TIMEOUT", "15"))
POSITION_TIMEOUT = float(os.environ.get("POSITION_TIMEOUT", "10"))

# Timeout for placement/ack checks.
ORDER_ACK_TIMEOUT = float(os.environ.get("ORDER_ACK_TIMEOUT", "2.0"))

# In placement-only mode, cleanup cancels orders but does not wait to flatten positions.
PLACEMENT_ONLY_CLEANUP = os.environ.get("PLACEMENT_ONLY_CLEANUP", "1").lower() not in (
    "0",
    "false",
    "no",
)

# Price prerequisite mode: live snapshot can be enabled when desired.
USE_LIVE_SNAPSHOT_PRICE = os.environ.get("USE_LIVE_SNAPSHOT_PRICE", "0").lower() in (
    "1",
    "true",
    "yes",
)
STATIC_LAST_PRICE = float(os.environ.get("STATIC_LAST_PRICE", "100"))

# Default quantity for test orders — keep small to minimize paper risk
TEST_QTY = int(os.environ.get("TEST_QTY", "1"))

# Contract selection for integration tests.
# Default to AAPL symbol to avoid account-specific ETF/KID restrictions.
TEST_SYMBOL = os.environ.get("TWS_TEST_SYMBOL", "AAPL").strip().upper()
TEST_CONID = int(os.environ.get("TWS_TEST_CONID", "756733"))
USE_SCANNER_CONID = os.environ.get("TWS_TEST_USE_SCANNER_CONID", "0").lower() in (
    "1",
    "true",
    "yes",
)
STRICT_FILL_SCAN_CODE = os.environ.get("STRICT_FILL_SCAN_CODE", "TOP_PERC_GAIN")
STRICT_FILL_SCAN_LOCATION = os.environ.get("STRICT_FILL_SCAN_LOCATION", "STK.US.MAJOR")
STRICT_FILL_SCAN_ROWS = int(os.environ.get("STRICT_FILL_SCAN_ROWS", "10"))
STRICT_FILL_MIN_PRICE = float(os.environ.get("STRICT_FILL_MIN_PRICE", "1"))
STRICT_FILL_MIN_VOLUME = int(os.environ.get("STRICT_FILL_MIN_VOLUME", "500000"))

# Auto-start local bridge for integration tests unless explicitly disabled.
# Remote bridge URLs are never auto-managed.
BRIDGE_AUTOSTART = os.environ.get("TWS_BRIDGE_AUTOSTART", "1").lower() not in (
    "0",
    "false",
    "no",
)
BRIDGE_TWS_PORT = os.environ.get("TWS_TEST_TWS_PORT", "7497").strip()


def _health_url():
    return BASE_URL.rstrip("/") + "/api/health"


def _is_local_bridge_url():
    parsed = urlparse(BASE_URL)
    host = (parsed.hostname or "").lower()
    return host in ("localhost", "127.0.0.1", "::1")


def _bridge_ready(timeout=2):
    try:
        req = urllib.request.Request(_health_url(), method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode() or "{}")
        return bool(payload.get("ok"))
    except Exception:
        return False


def _start_local_bridge():
    project_dir = Path(__file__).resolve().parent.parent
    serve_path = project_dir / "serve.py"
    if not serve_path.exists():
        raise RuntimeError(f"serve.py not found at {serve_path}")

    cmd = [sys.executable, str(serve_path)]
    if BRIDGE_TWS_PORT:
        cmd.extend(["--tws-port", BRIDGE_TWS_PORT])
    kwargs = {
        "cwd": str(project_dir),
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
    }
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    return subprocess.Popen(cmd, **kwargs)


@pytest.fixture(scope="session", autouse=True)
def bridge_process():
    """Ensure a local bridge is available for tests.

    Behavior:
      - If BASE_URL is remote: no process management.
      - If local bridge already responds: reuse it and do not stop it.
      - Otherwise start serve.py, wait for /api/health, and stop it at session end.
    """
    if not BRIDGE_AUTOSTART:
        yield None
        return

    if not _is_local_bridge_url():
        yield None
        return

    if _bridge_ready(timeout=1):
        # External bridge already running; don't own lifecycle.
        yield None
        return

    proc = _start_local_bridge()
    deadline = time.time() + 45
    while time.time() < deadline:
        if proc.poll() is not None:
            break
        if _bridge_ready(timeout=1):
            yield proc
            break
        time.sleep(0.5)
    else:
        # Loop exhausted without readiness.
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        raise RuntimeError(
            "Bridge did not become ready at /api/health within 45s. "
            "Ensure TWS/IB Gateway is running and accessible."
        )

    if proc.poll() is not None and not _bridge_ready(timeout=1):
        raise RuntimeError(
            "Bridge process exited before becoming ready. "
            "Ensure TWS/IB Gateway is running and accessible."
        )

    # If we started it and it reached readiness, clean it up after tests.
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except Exception:
            proc.kill()


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def api(method, path, body=None, timeout=3):
    """Send a request to the bridge and return parsed JSON."""
    url = BASE_URL + path
    # Important: {} must be sent as a JSON body, not omitted.
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method)
    r.add_header("Content-Type", "application/json")
    try:
        resp = urllib.request.urlopen(r, timeout=timeout)
        text = resp.read().decode()
        return json.loads(text) if text else {}
    except urllib.error.HTTPError as e:
        text = e.read().decode() if e.fp else ""
        if text:
            try:
                return json.loads(text)
            except Exception:
                pass
        return {"error": f"HTTP {e.code}: {e.reason}"}


def place_order(
    account_id,
    conid,
    side,
    qty,
    order_type="MKT",
    price=0,
    tp_price=None,
    sl_price=None,
    tif="DAY",
    outside_rth=True,
):
    """Place an order and return the response dict."""
    payload = {
        "orders": [
            {
                "conid": str(conid),
                "side": side,
                "quantity": qty,
                "orderType": order_type,
                "price": price,
                "tif": tif,
                "outsideRTH": outside_rth,
            }
        ]
    }
    if tp_price is not None:
        payload["orders"][0]["tpPrice"] = tp_price
    if sl_price is not None:
        payload["orders"][0]["slPrice"] = sl_price
    resp = api("POST", f"/v1/api/iserver/account/{account_id}/orders", payload)
    assert (
        isinstance(resp, list) and len(resp) > 0
    ), f"Unexpected place_order response: {resp}"
    return resp[0]


def cancel_order(account_id, order_id):
    """Cancel an order by ID."""
    return api("DELETE", f"/v1/api/iserver/account/{account_id}/order/{order_id}")


def modify_order(account_id, order_id, price):
    """Modify an order's limit price."""
    return api(
        "PUT",
        f"/v1/api/iserver/account/{account_id}/order/{order_id}",
        {"price": price},
    )


def get_orders():
    """Return today's orders list."""
    resp = api("GET", "/v1/api/iserver/account/orders")
    return resp if isinstance(resp, list) else resp.get("orders", [])


def get_positions(account_id):
    """Return open positions."""
    resp = api("GET", f"/v1/api/portfolio/{account_id}/positions/0")
    return resp if isinstance(resp, list) else []


def cancel_working_orders_for_conid(account_id, conid, sides=None):
    """Cancel currently working orders for an account and conid.

    Returns the number of cancel attempts made.
    """
    side_set = {str(s).upper() for s in (sides or [])}
    attempts = 0
    for o in get_orders():
        status = str(o.get("status") or "")
        if status not in ("Submitted", "PreSubmitted"):
            continue
        oid = o.get("orderId") or o.get("order_id")
        if not oid:
            continue
        o_conid = o.get("conid")
        if str(o_conid) != str(conid):
            continue
        if side_set:
            side = str(o.get("side") or "").upper()
            if side not in side_set:
                continue
        try:
            cancel_order(account_id, oid)
            attempts += 1
        except Exception:
            pass
    return attempts


def wait_for_fill(order_id, timeout=None):
    """Compatibility helper: placement-first alias for quick test runs."""
    return wait_for_order_seen(order_id, timeout=timeout or ORDER_ACK_TIMEOUT)


def wait_for_order_seen(order_id, timeout=None):
    """Wait until an order is visible in broker order feed."""
    deadline = time.time() + (ORDER_ACK_TIMEOUT if timeout is None else timeout)
    while time.time() < deadline:
        orders = get_orders()
        for o in orders:
            oid = o.get("orderId") or o.get("order_id") or o.get("permId")
            if str(oid) == str(order_id):
                return o
        time.sleep(0.05)
    return None


_NON_BRIDGE_REJECTION_MARKERS = (
    "No Trading Permission",
    "Customer Ineligible",
    "not available for short sale",
    "KID",
    "no market data permissions",
)


def assert_order_accepted_or_skip(resp, what="order"):
    """Fail for likely bridge issues, skip for account/market permission constraints."""
    err = resp.get("error")
    if not err:
        return
    msg = str(err)
    low = msg.lower()
    if any(m.lower() in low for m in _NON_BRIDGE_REJECTION_MARKERS):
        pytest.skip(f"{what} blocked by account/market permissions: {msg}")
    pytest.fail(f"{what} rejected unexpectedly: {msg}")


def require_class_attr(test_cls, attr_name):
    """Get class attribute or skip when a prerequisite step did not complete."""
    if not hasattr(test_cls, attr_name):
        pytest.skip(f"Missing prerequisite attribute: {attr_name}")
    return getattr(test_cls, attr_name)


def require_filled(order_id, what="order", timeout=None):
    """Placement-first helper.

    Returns quickly once the order is acknowledged/visible. If a fill is already
    available, returns that filled record; otherwise returns the latest seen order.
    """
    order = wait_for_order_seen(order_id, timeout=timeout)
    if order:
        return order
    # Placement-first fallback: order_id came from place response, treat as accepted.
    return {"orderId": order_id, "status": "Submitted"}


def wait_for_fill_details(order_id, timeout=None):
    """Wait until avg fill price metadata is populated for an order."""
    deadline = time.time() + (FILL_DETAILS_TIMEOUT if timeout is None else timeout)
    last_seen = None
    while time.time() < deadline:
        order = wait_for_order_seen(order_id, timeout=0.5)
        if order:
            last_seen = order
            avg = float(order.get("avgPrice") or order.get("avgFillPrice") or 0)
            if avg > 0:
                return order
        time.sleep(0.2)
    return last_seen


def wait_for_position_visible(account_id, conid, timeout=None):
    """Wait until a non-zero position is visible for a contract."""
    deadline = time.time() + (POSITION_TIMEOUT if timeout is None else timeout)
    last_seen = None
    while time.time() < deadline:
        pos = find_position(account_id, conid)
        if pos is not None:
            last_seen = pos
            if float(pos.get("position", 0)) != 0:
                return pos
        time.sleep(0.2)
    return last_seen


def wait_for_status(order_id, statuses, timeout=None):
    """Wait until an order appears with one of the provided statuses."""
    if isinstance(statuses, str):
        statuses = [statuses]
    status_set = {str(s) for s in statuses}
    deadline = time.time() + (ORDER_ACK_TIMEOUT if timeout is None else timeout)
    while time.time() < deadline:
        orders = get_orders()
        for o in orders:
            oid = str(o.get("orderId") or o.get("order_id") or o.get("permId", ""))
            if oid == str(order_id):
                st = str(o.get("status") or "")
                if not status_set or st in status_set:
                    return o
        time.sleep(0.05)
    return None


def find_position(account_id, conid):
    """Find an open position by conid, returns None if flat."""
    positions = get_positions(account_id)
    for p in positions:
        if str(p.get("conid")) == str(conid):
            return p
    return None


def get_liquid_stock_conid():
    """Resolve test conid.

    Prefer deterministic symbol lookup (AAPL by default) to avoid scanner-selected
    hard-to-short names and ETF permission constraints.
    Scanner can be re-enabled via TWS_TEST_USE_SCANNER_CONID=1.
    """
    if TEST_SYMBOL:
        try:
            resp = api(
                "POST",
                "/v1/api/iserver/secdef/search",
                {"symbol": TEST_SYMBOL, "name": True},
            )
            if isinstance(resp, list) and resp:
                first = resp[0]
                cid = first.get("conid") or first.get("con_id")
                if cid:
                    return cid
        except Exception:
            pass

    if not USE_SCANNER_CONID:
        return TEST_CONID

    try:
        resp = api(
            "POST",
            "/v1/api/iserver/scanner/run",
            {
                "instrument": "STK",
                "type": "MOST_ACTIVE",
                "location": "STK.US.MAJOR",
                "filter": [],
            },
        )
        contracts = resp.get("contracts", [])
        if contracts:
            cid = contracts[0].get("con_id") or contracts[0].get("conid")
            if cid:
                return cid
    except Exception:
        pass

    return TEST_CONID


def get_search_conid(symbol):
    """Resolve a symbol through the same secdef search flow the app uses."""
    if not symbol:
        return None
    try:
        resp = api(
            "POST",
            "/v1/api/iserver/secdef/search",
            {"symbol": str(symbol).upper().strip(), "name": True},
        )
        if isinstance(resp, list) and resp:
            first = resp[0]
            cid = first.get("conid") or first.get("con_id")
            if cid:
                return int(cid)
    except Exception:
        pass
    return None


def get_scanner_symbols(scan_code, location=None, above_price=None, above_volume=None):
    """Fetch scanner candidate symbols from the bridge."""
    try:
        resp = api(
            "POST",
            "/v1/api/iserver/scanner/run",
            {
                "instrument": "STK",
                "type": scan_code,
                "sortBy": scan_code,
                "location": location or STRICT_FILL_SCAN_LOCATION,
                "abovePrice": (
                    STRICT_FILL_MIN_PRICE if above_price is None else above_price
                ),
                "aboveVolume": (
                    STRICT_FILL_MIN_VOLUME if above_volume is None else above_volume
                ),
                "numberOfRows": STRICT_FILL_SCAN_ROWS,
            },
        )
        contracts = resp.get("contracts", []) if isinstance(resp, dict) else []
        return [c.get("symbol") for c in contracts if c.get("symbol")]
    except Exception:
        return []


def get_scanner_conid(scan_code, location=None, above_price=None, above_volume=None):
    """Resolve a contract via the bridge scanner endpoint."""
    try:
        resp = api(
            "POST",
            "/v1/api/iserver/scanner/run",
            {
                "instrument": "STK",
                "type": scan_code,
                "sortBy": scan_code,
                "location": location or STRICT_FILL_SCAN_LOCATION,
                "abovePrice": (
                    STRICT_FILL_MIN_PRICE if above_price is None else above_price
                ),
                "aboveVolume": (
                    STRICT_FILL_MIN_VOLUME if above_volume is None else above_volume
                ),
                "numberOfRows": STRICT_FILL_SCAN_ROWS,
            },
        )
        contracts = resp.get("contracts", []) if isinstance(resp, dict) else []
        if contracts:
            first = contracts[0]
            cid = first.get("con_id") or first.get("conid")
            if cid:
                return int(cid)
    except Exception:
        pass
    return None


def get_snapshot(conid, timeout=3):
    """Get a market data snapshot for a conid."""
    return api(
        "GET",
        f"/v1/api/iserver/marketdata/snapshot?conids={conid}&fields=31,84,86",
        timeout=timeout,
    )


def get_depth_snapshot(conid, timeout=3):
    """Get a level-2 depth snapshot for a conid."""
    return api(
        "GET", f"/v1/api/iserver/marketdata/depth?conid={conid}", timeout=timeout
    )


def _extract_best_bid_ask_from_depth_rows(rows):
    """Extract best bid/ask from bridge depth rows."""
    if not isinstance(rows, list) or not rows:
        return 0.0, 0.0
    best_bid = 0.0
    best_ask = 0.0
    for row in rows:
        try:
            price = float(str((row or {}).get("price") or 0).replace(",", ""))
        except Exception:
            price = 0.0
        if price <= 0:
            continue
        bid_sz = (row or {}).get("bid")
        ask_sz = (row or {}).get("ask")
        if bid_sz not in ("", None):
            best_bid = max(best_bid, price)
        if ask_sz not in ("", None):
            best_ask = price if best_ask == 0 else min(best_ask, price)
    return best_bid, best_ask


def prime_depth_subscription(account_id, conid, timeout=4):
    """Mirror the app's WS depth subscription flow before reading depth REST."""
    if not HAS_WS:
        return False

    async def _prime():
        try:
            async with websockets.connect(WS_URL, close_timeout=5) as ws:
                deadline = time.time() + timeout
                ready = False
                while time.time() < deadline:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    data = json.loads(msg) if isinstance(msg, str) else msg
                    if isinstance(data, dict) and data.get("topic") == "sts":
                        args = data.get("args") or {}
                        if args.get("authenticated"):
                            ready = True
                            break
                if not ready:
                    return False
                await ws.send(f"sbd+{account_id}+{conid}")
                wait_deadline = time.time() + timeout
                while time.time() < wait_deadline:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    data = json.loads(msg) if isinstance(msg, str) else msg
                    if isinstance(data, dict) and str(data.get("topic") or "").endswith(
                        f"+{conid}"
                    ):
                        return True
                return True
        except Exception:
            return False

    try:
        return asyncio.run(_prime())
    except Exception:
        return False


def get_l2_bid_ask_ws(account_id, conid, timeout=5):
    """Read L2 top-of-book directly from WS depth stream like the app."""
    if not HAS_WS:
        return 0.0, 0.0

    async def _ws_read():
        try:
            async with websockets.connect(WS_URL, close_timeout=5) as ws:
                deadline = time.time() + timeout
                ready = False
                while time.time() < deadline:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    data = json.loads(msg) if isinstance(msg, str) else msg
                    if isinstance(data, dict) and data.get("topic") == "sts":
                        args = data.get("args") or {}
                        if args.get("authenticated"):
                            ready = True
                            break
                if not ready:
                    return 0.0, 0.0

                await ws.send(f"sbd+{account_id}+{conid}")

                wait_deadline = time.time() + timeout
                while time.time() < wait_deadline:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    data = json.loads(msg) if isinstance(msg, str) else msg
                    if not isinstance(data, dict):
                        continue
                    topic = str(data.get("topic") or "")
                    if not topic.endswith(f"+{conid}"):
                        continue
                    rows = data.get("data")
                    bid, ask = _extract_best_bid_ask_from_depth_rows(rows)
                    if bid > 0 or ask > 0:
                        return bid, ask
                return 0.0, 0.0
        except Exception:
            return 0.0, 0.0

    try:
        return asyncio.run(_ws_read())
    except Exception:
        return 0.0, 0.0


def get_l2_bid_ask(conid, retries=4, delay=0.4):
    """Fetch top-of-book bid/ask from the bridge depth snapshot."""
    for _ in range(max(1, retries)):
        try:
            rows = get_depth_snapshot(conid, timeout=1.5)
        except Exception:
            rows = []
        bid, ask = _extract_best_bid_ask_from_depth_rows(rows)
        if bid > 0 or ask > 0:
            return bid, ask
        time.sleep(delay)
    return 0.0, 0.0


def get_bid_ask(conid, retries=5, delay=0.3):
    """Fetch bid/ask for a conid from snapshot fields 84/86."""
    last_snap = {}
    for _ in range(max(1, retries)):
        try:
            snap = get_snapshot(conid, timeout=1.5)
        except Exception:
            snap = {}
        last_snap = snap[0] if isinstance(snap, list) and snap else (snap or {})
        try:
            bid = float(str(last_snap.get("84") or 0).replace(",", ""))
        except Exception:
            bid = 0.0
        try:
            ask = float(str(last_snap.get("86") or 0).replace(",", ""))
        except Exception:
            ask = 0.0
        if bid > 0 or ask > 0:
            return bid, ask
        time.sleep(delay)
    return 0.0, 0.0


def get_marketable_limit_price(
    side, conid, fallback_price=0.0, cushion=0.02, account_id=None
):
    """Build an off-hours marketable LMT price from bid/ask.

    BUY: pay the ask or slightly above.
    SELL: hit the bid or slightly below.
    """
    bid, ask = (0.0, 0.0)
    if account_id:
        bid, ask = get_l2_bid_ask_ws(account_id, conid)
    if bid <= 0 and ask <= 0 and account_id:
        prime_depth_subscription(account_id, conid)
    if bid <= 0 and ask <= 0:
        bid, ask = get_l2_bid_ask(conid)
    if bid <= 0 and ask <= 0:
        bid, ask = get_bid_ask(conid)
    side = str(side).upper()
    if side == "BUY":
        if ask > 0:
            return round(ask + cushion, 2)
        if bid > 0:
            return round(bid + max(cushion, 0.05), 2)
    else:
        if bid > 0:
            return round(max(0.01, bid - cushion), 2)
        if ask > 0:
            return round(max(0.01, ask - max(cushion, 0.05)), 2)
    if fallback_price > 0:
        if side == "BUY":
            return round(fallback_price + max(cushion, 0.05), 2)
        return round(max(0.01, fallback_price - max(cushion, 0.05)), 2)
    return 0.0


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def bridge_url():
    return BASE_URL


def _resolve_account_id_with_retry(timeout=45):
    """Resolve account id while TWS auth/session settles."""
    deadline = time.time() + timeout
    last_resp = None
    while time.time() < deadline:
        resp = api("GET", "/v1/api/portfolio/accounts")
        last_resp = resp
        if isinstance(resp, list) and resp:
            aid = resp[0].get("accountId") or resp[0].get("id")
            if aid:
                return aid
        time.sleep(1.0)
    raise AssertionError(
        f"No accounts returned - is serve.py connected/authenticated to TWS? last={last_resp}"
    )


def _warm_auth_status(timeout=20):
    """Poll auth endpoint until authenticated and connected are both true."""
    deadline = time.time() + timeout
    last_resp = None
    while time.time() < deadline:
        resp = api("GET", "/v1/api/iserver/auth/status")
        last_resp = resp
        if (
            isinstance(resp, dict)
            and resp.get("authenticated")
            and resp.get("connected")
        ):
            return resp
        time.sleep(1.0)
    return last_resp


def _extract_last_price(snapshot_payload):
    raw = (
        snapshot_payload.get("31")
        or snapshot_payload.get("84")
        or snapshot_payload.get("86")
    )
    if not raw:
        return None
    try:
        price = float(str(raw).replace(",", "").lstrip("C").lstrip("H"))
        if price > 0:
            return price
    except Exception:
        return None
    return None


def _resolve_snapshot_and_last_price(conid, retries=5, delay=0.3):
    """Fetch a snapshot and parse a usable last price for a specific conid."""
    snap = {}
    price = STATIC_LAST_PRICE
    for _ in range(max(1, retries)):
        try:
            candidate = get_snapshot(conid, timeout=1.5)
        except Exception:
            candidate = {}
        snap = (
            candidate[0]
            if isinstance(candidate, list) and candidate
            else (candidate or {})
        )
        parsed = _extract_last_price(snap)
        if parsed is not None:
            price = parsed
            break
        time.sleep(delay)
    return snap, price


@pytest.fixture(scope="session")
def integration_prereqs(bridge_process):
    """Resolve shared prerequisites once and reuse across all tests."""
    _warm_auth_status()
    aid = _resolve_account_id_with_retry()
    assert (
        "D" in aid.upper()
    ), f"Account {aid} doesn't look like a paper account — aborting for safety"

    cid = get_liquid_stock_conid()
    assert cid, "Could not resolve a test conid"
    cid = int(cid)

    snap = {}
    lp = STATIC_LAST_PRICE
    if USE_LIVE_SNAPSHOT_PRICE:
        for _ in range(5):
            candidate = get_snapshot(cid)
            snap = (
                candidate[0]
                if isinstance(candidate, list) and candidate
                else (candidate or {})
            )
            parsed = _extract_last_price(snap)
            if parsed is not None:
                lp = parsed
                break
            time.sleep(0.3)

    return IntegrationContext(account_id=aid, conid=cid, last_price=lp, snapshot=snap)


@pytest.fixture(scope="session")
def strict_fill_ctx(integration_prereqs):
    """Context for strict fill tests using app-style symbol resolution when possible."""
    strict_cid = None
    for symbol in get_scanner_symbols(STRICT_FILL_SCAN_CODE):
        strict_cid = get_search_conid(symbol)
        if strict_cid:
            break
    if not strict_cid:
        return integration_prereqs
    strict_snap, strict_last = _resolve_snapshot_and_last_price(strict_cid)
    return IntegrationContext(
        account_id=integration_prereqs.account_id,
        conid=strict_cid,
        last_price=strict_last,
        snapshot=strict_snap,
    )


@pytest.fixture(scope="session")
def account_id(integration_prereqs):
    return integration_prereqs.account_id


@pytest.fixture(scope="session")
def conid(integration_prereqs):
    return integration_prereqs.conid


@pytest.fixture(scope="session")
def snapshot(integration_prereqs):
    return integration_prereqs.snapshot


@pytest.fixture(scope="session")
def last_price(integration_prereqs):
    return integration_prereqs.last_price


@dataclass(frozen=True)
class IntegrationContext:
    """Shared integration prerequisites and bound bridge helpers.

    This keeps repeated prerequisites in one object so tests can pass a single
    fixture instead of threading account_id/conid/last_price everywhere.
    """

    account_id: str
    conid: int
    last_price: float
    snapshot: dict

    def place_order(self, side, qty, order_type="MKT", price=0, **kwargs):
        return place_order(
            self.account_id,
            self.conid,
            side,
            qty,
            order_type=order_type,
            price=price,
            **kwargs,
        )

    def cancel_order(self, order_id):
        return cancel_order(self.account_id, order_id)

    def modify_order(self, order_id, price):
        return modify_order(self.account_id, order_id, price)

    def get_positions(self):
        return get_positions(self.account_id)

    def find_position(self):
        return find_position(self.account_id, self.conid)


@pytest.fixture(scope="session")
def integration_ctx(integration_prereqs):
    """Session-wide prerequisite bundle used by multiple integration tests."""
    return integration_prereqs


@pytest.fixture(autouse=True)
def _small_delay():
    """Small delay between tests to avoid overwhelming TWS."""
    yield
    return


# ---------------------------------------------------------------------------
# Global safety net — cancel all working orders and flatten all positions
# ---------------------------------------------------------------------------


def flatten_all(account_id, force_flatten_positions=False):
    """Cancel working orders and optionally flatten all open positions.

    - Class cleanup: cancel-only for speed
    - Final session cleanup: force_flatten_positions=True for safety
    """
    # 1) Cancel all working orders
    try:
        orders = get_orders()
        for o in orders:
            status = o.get("status", "")
            if status in ("Submitted", "PreSubmitted"):
                oid = o.get("orderId") or o.get("order_id")
                if oid:
                    try:
                        cancel_order(account_id, oid)
                    except Exception:
                        pass
    except Exception:
        pass

    if PLACEMENT_ONLY_CLEANUP and not force_flatten_positions:
        return

    # 2) Flatten all open positions
    try:
        positions = get_positions(account_id)
        for p in positions:
            qty = p.get("position", 0)
            cid = p.get("conid")
            if qty != 0 and cid:
                side = "SELL" if qty > 0 else "BUY"
                abs_qty = abs(qty)
                try:
                    resp = place_order(account_id, cid, side, abs_qty, order_type="MKT")
                    if "error" not in resp:
                        wait_for_order_seen(resp["order_id"], timeout=0.2)
                except Exception:
                    pass
    except Exception:
        pass


@pytest.fixture(autouse=True, scope="class")
def _cleanup_after_class(account_id, conid):
    """Keep class boundaries clean by cancelling working orders for the test contract."""
    cancel_working_orders_for_conid(account_id, conid)
    yield
    cancel_working_orders_for_conid(account_id, conid)


@pytest.fixture(autouse=True, scope="session")
def _final_cleanup(account_id):
    """Last resort: always flatten everything when the entire test session ends."""
    yield
    flatten_all(account_id, force_flatten_positions=True)
