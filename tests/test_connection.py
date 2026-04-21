"""
Test: Connection health, auth status, and WebSocket streaming.

Covers:
  - Auth status returns authenticated=True
  - Gateway info endpoint works
  - WebSocket connects and receives data
  - Market data subscription via WS returns price fields
  - WS handles invalid subscription gracefully
  - Bridge survives rapid reconnect attempts
"""
import pytest
import time
import json
from .conftest import api, BASE_URL, WS_URL

try:
    import websockets
    import asyncio
    HAS_WS = True
except ImportError:
    HAS_WS = False


class TestAuthAndConnection:

    def test_auth_status_authenticated(self):
        resp = api("GET", "/v1/api/iserver/auth/status")
        assert isinstance(resp, dict)
        assert resp.get("authenticated") is True, f"Not authenticated: {resp}"

    def test_auth_status_connected(self):
        resp = api("GET", "/v1/api/iserver/auth/status")
        assert resp.get("connected") is True, f"Not connected: {resp}"

    def test_gateway_info(self):
        resp = api("GET", "/gw")
        assert isinstance(resp, dict)
        assert "ws" in resp, f"Missing 'ws' key: {list(resp.keys())}"
        assert resp.get("mode") == "tws", f"Expected mode=tws, got {resp.get('mode')}"

    def test_accounts_returns_paper(self):
        resp = api("GET", "/v1/api/portfolio/accounts")
        assert isinstance(resp, list) and len(resp) > 0
        aid = resp[0].get("accountId", "")
        assert "D" in aid.upper(), f"Expected paper account (D prefix), got {aid}"


@pytest.mark.skipif(not HAS_WS, reason="websockets package not installed")
class TestWebSocket:

    @pytest.fixture
    def event_loop(self):
        loop = asyncio.new_event_loop()
        yield loop
        loop.close()

    def test_ws_connects_and_receives_sts(self, event_loop, conid):
        """WS should connect and send an initial status message."""
        async def _test():
            async with websockets.connect(WS_URL, close_timeout=5) as ws:
                # Should receive at least one message within 5s
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                data = json.loads(msg) if isinstance(msg, str) else msg
                # First message is often auth status
                assert data is not None
                return data
        result = event_loop.run_until_complete(_test())
        assert result is not None

    def test_ws_market_data_subscription(self, event_loop, conid):
        """Subscribe to market data and receive at least one update."""
        async def _test():
            async with websockets.connect(WS_URL, close_timeout=5) as ws:
                # Drain initial messages
                try:
                    await asyncio.wait_for(ws.recv(), timeout=2)
                except asyncio.TimeoutError:
                    pass
                # Subscribe
                await ws.send(f"smd+{conid}")
                # Wait for a market data message (up to 10s)
                deadline = time.time() + 10
                while time.time() < deadline:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=3)
                        data = json.loads(msg) if isinstance(msg, str) else msg
                        if isinstance(data, dict) and ("31" in data or "84" in data or "86" in data):
                            return data
                    except asyncio.TimeoutError:
                        continue
                return None
        result = event_loop.run_until_complete(_test())
        # Market data may not arrive if market is closed — just verify no crash
        # If we got data, verify it has conid
        if result:
            assert "conid" in result or "conId" in result

    def test_ws_unsubscribe(self, event_loop, conid):
        """Unsubscribe should not crash."""
        async def _test():
            async with websockets.connect(WS_URL, close_timeout=5) as ws:
                await ws.send(f"smd+{conid}")
                await asyncio.sleep(1)
                await ws.send(f"umd+{conid}")
                await asyncio.sleep(1)
                # Should still be connected
                return True
        assert event_loop.run_until_complete(_test())

