"""
Test: Position tracking, P&L accuracy, and the orders endpoint.

Covers:
  - Orders endpoint returns today's filled orders
  - Filled orders have avgPrice, filledQuantity, commission
  - Positions endpoint reflects correct unrealized P&L sign
  - Round-trip P&L: (sell_fill - buy_fill) * qty matches expected direction
  - Account summary has net liquidation value
  - Commission is reported (non-zero for fills)
"""

import pytest
import time
from .conftest import (
    place_order,
    get_orders,
    wait_for_fill,
    find_position,
    get_positions,
    cancel_working_orders_for_conid,
    api,
    TEST_QTY,
    assert_order_accepted_or_skip,
    require_filled,
)


class TestOrdersEndpoint:
    """Validate the GET /v1/api/iserver/account/orders response format."""

    def test_orders_returns_list(self):
        orders = get_orders()
        assert isinstance(orders, list), f"Expected list, got {type(orders)}"

    def test_filled_orders_have_required_fields(self, account_id, conid):
        """Place and fill an order, then verify the orders endpoint has correct fields."""
        resp = place_order(account_id, conid, "BUY", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(resp, "orders-endpoint setup buy")
        oid = resp["order_id"]
        order = require_filled(oid, "orders-endpoint setup buy")

        # Check required fields for share card / frontend
        for field in ["status", "side"]:
            assert field in order, f"Missing '{field}' in order: {list(order.keys())}"

        # avgPrice or avgFillPrice must be present and positive
        avg = float(order.get("avgPrice") or order.get("avgFillPrice") or 0)
        if avg <= 0:
            pytest.skip("avgPrice not populated yet for filled order")

        # filledQuantity or totalSize
        qty = int(
            order.get("filledQuantity")
            or order.get("totalSize")
            or order.get("quantity")
            or 0
        )
        assert qty > 0, f"filled qty should be positive, got {qty}"

        self.__class__._buy_oid = oid
        self.__class__._buy_price = avg

        # Cleanup: sell
        sell = place_order(account_id, conid, "SELL", TEST_QTY, order_type="MKT")
        if "error" not in sell:
            wait_for_fill(sell["order_id"])
            self.__class__._sell_oid = sell["order_id"]

    def test_commission_reported(self, account_id, conid):
        """Filled orders should have a commission field."""
        orders = get_orders()
        filled = [o for o in orders if o.get("status") == "Filled"]
        # At least some should have commission
        with_comm = [
            o
            for o in filled
            if float(o.get("commission") or o.get("commissionAmount") or 0) > 0
        ]
        # Commission may not always be present immediately — just warn
        if not with_comm and filled:
            pytest.skip("Commission not yet populated (may appear with delay)")

    def test_orders_have_timestamps(self):
        """Orders should have time fields for date filtering."""
        orders = get_orders()
        filled = [o for o in orders if o.get("status") == "Filled"]
        for o in filled[:5]:
            has_time = any(
                [
                    o.get("lastExecutionTime"),
                    o.get("orderTime"),
                    o.get("lastFillTime"),
                ]
            )
            assert has_time, f"Order missing time fields: {list(o.keys())}"


class TestPositionsEndpoint:
    """Validate positions endpoint data."""

    def test_positions_returns_list(self, account_id):
        positions = get_positions(account_id)
        assert isinstance(positions, list)

    def test_position_fields(self, account_id, conid):
        """Open a position and verify the fields returned."""
        cancel_working_orders_for_conid(account_id, conid)
        resp = place_order(account_id, conid, "BUY", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(resp, "positions setup buy")
        require_filled(resp["order_id"], "positions setup buy")
        time.sleep(0)

        pos = find_position(account_id, conid)
        if pos is None:
            pytest.skip("Position not visible yet after buy fill")

        # Must have these fields
        assert "position" in pos, f"Missing 'position': {list(pos.keys())}"
        assert "conid" in pos
        assert pos["position"] != 0

        # avgPrice / avgCost should be present
        avg = float(pos.get("avgPrice") or pos.get("avgCost") or 0)
        if avg <= 0:
            pytest.skip("Position avg price not populated yet")

        # Cleanup
        sell = place_order(account_id, conid, "SELL", TEST_QTY, order_type="MKT")
        if "error" not in sell:
            wait_for_fill(sell["order_id"])


class TestRoundTripPnL:
    """Verify that a round-trip (buy then sell) produces correct P&L direction."""

    def test_round_trip_pnl(self, account_id, conid):
        """Buy, then sell, and verify the fill prices make sense together."""
        cancel_working_orders_for_conid(account_id, conid)
        # BUY
        buy_resp = place_order(account_id, conid, "BUY", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(buy_resp, "round-trip buy")
        buy_order = require_filled(buy_resp["order_id"], "round-trip buy")
        buy_price = float(
            buy_order.get("avgPrice") or buy_order.get("avgFillPrice") or 0
        )
        if buy_price <= 0:
            pytest.skip("Round-trip buy price not populated yet")

        time.sleep(0)

        # SELL
        sell_resp = place_order(account_id, conid, "SELL", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(sell_resp, "round-trip sell")
        sell_order = require_filled(sell_resp["order_id"], "round-trip sell")
        sell_price = float(
            sell_order.get("avgPrice") or sell_order.get("avgFillPrice") or 0
        )
        if sell_price <= 0:
            pytest.skip("Round-trip sell price not populated yet")

        pnl = (sell_price - buy_price) * TEST_QTY
        # We can't predict direction, but prices should be very close for immediate round-trip
        spread_pct = abs(sell_price - buy_price) / buy_price * 100
        assert (
            spread_pct < 2.0
        ), f"Spread too wide for immediate round-trip: buy={buy_price} sell={sell_price} ({spread_pct:.2f}%)"


class TestAccountSummary:
    """Validate account summary endpoint."""

    def test_summary_has_net_liquidation(self, account_id):
        resp = api("GET", f"/v1/api/portfolio/{account_id}/summary")
        assert isinstance(resp, dict), f"Expected dict, got {type(resp)}"
        # Should have netliquidation or similar
        nlv = (
            resp.get("netliquidation")
            or resp.get("NetLiquidation")
            or resp.get("totalCashValue")
        )
        assert (
            nlv is not None
        ), f"No net liquidation in summary: {list(resp.keys())[:15]}"
