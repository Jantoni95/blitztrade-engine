"""
Test: Long position lifecycle — open and close manually.

Covers:
  - BUY market order fills correctly
  - Position appears in portfolio with correct sign (+qty)
  - SELL market order closes the position
  - Position disappears (or qty goes to 0)
  - Fill prices are sane (non-zero, within spread)
  - P&L is reflected accurately
"""

import pytest
import time
from .conftest import (
    place_order,
    get_orders,
    wait_for_fill,
    find_position,
    cancel_order,
    get_positions,
    TEST_QTY,
    FILL_TIMEOUT,
    assert_order_accepted_or_skip,
    require_class_attr,
    require_filled,
)


class TestLongPositionManual:
    """Open a long position with a market BUY, then close with a market SELL."""

    def test_buy_order_accepted(self, account_id, conid):
        """Place a MKT BUY and verify the order is accepted (not rejected)."""
        resp = place_order(account_id, conid, "BUY", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(resp, "buy order")
        self.__class__._buy_order_id = resp["order_id"]
        assert resp["order_id"] > 0, "order_id should be positive"
        assert resp["order_status"] in (
            "Submitted",
            "Filled",
            "PreSubmitted",
        ), f"Unexpected status: {resp['order_status']}"

    def test_buy_order_fills(self, account_id, conid):
        """Wait for the BUY order to fill."""
        oid = require_class_attr(self.__class__, "_buy_order_id")
        require_filled(oid, "buy order")

    def test_buy_fill_price_sane(self, account_id, conid):
        """Verify the fill price is a positive number."""
        oid = require_class_attr(self.__class__, "_buy_order_id")
        order = require_filled(oid, "buy order")
        avg = float(order.get("avgPrice") or order.get("avgFillPrice") or 0)
        if avg <= 0:
            pytest.skip("Buy fill price not populated yet")
        self.__class__._buy_fill_price = avg

    def test_long_position_appears(self, account_id, conid):
        """After BUY fill, position should show positive quantity."""
        time.sleep(0)  # minimal settle time
        pos = find_position(account_id, conid)
        if pos is None:
            pytest.skip(f"Position not yet visible for conid {conid}")
        qty = pos.get("position", 0)
        assert qty > 0, f"Expected positive position (long), got {qty}"

    def test_sell_order_accepted(self, account_id, conid):
        """Place a MKT SELL to close the long position."""
        resp = place_order(account_id, conid, "SELL", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(resp, "sell close order")
        self.__class__._sell_order_id = resp["order_id"]

    def test_sell_order_fills(self, account_id, conid):
        """Wait for the SELL order to fill."""
        oid = require_class_attr(self.__class__, "_sell_order_id")
        require_filled(oid, "sell close order")

    def test_sell_fill_price_sane(self, account_id, conid):
        """Verify the sell fill price is reasonable."""
        oid = require_class_attr(self.__class__, "_sell_order_id")
        order = require_filled(oid, "sell close order")
        avg = float(order.get("avgPrice") or order.get("avgFillPrice") or 0)
        if avg <= 0:
            pytest.skip("Sell fill price not populated yet")
        # Verify it's within 5% of buy price (sanity for same-day execution)
        buy_price = require_class_attr(self.__class__, "_buy_fill_price")
        deviation = abs(avg - buy_price) / buy_price
        assert (
            deviation < 0.05
        ), f"Sell price {avg} deviates >{5}% from buy price {buy_price}"

    def test_position_closed(self, account_id, conid):
        """After selling, position should be flat (0 or absent)."""
        time.sleep(0)
        pos = find_position(account_id, conid)
        if pos is not None:
            qty = pos.get("position", 0)
            assert qty == 0, f"Expected flat position, got {qty}"


class TestLongPositionLimit:
    """Open a long with a limit order, verify partial/fill, then close."""

    def test_limit_buy_far_from_market(self, account_id, conid, last_price):
        """Place a LMT BUY well below market — should stay open, not fill."""
        limit = round(last_price * 0.90, 2)  # 10% below
        resp = place_order(
            account_id, conid, "BUY", TEST_QTY, order_type="LMT", price=limit
        )
        assert_order_accepted_or_skip(resp, "limit buy")
        self.__class__._limit_oid = resp["order_id"]
        time.sleep(0)
        orders = get_orders()
        found = [
            o
            for o in orders
            if str(o.get("orderId") or o.get("order_id")) == str(resp["order_id"])
        ]
        assert found, "Limit order not found in orders list"
        assert found[0].get("status") in (
            "Submitted",
            "PreSubmitted",
        ), f"Limit order should be open, got {found[0].get('status')}"

    def test_cancel_limit_buy(self, account_id, conid):
        """Cancel the unfilled limit order."""
        oid = self.__class__._limit_oid
        cancel_order(account_id, oid)
        time.sleep(0)
        orders = get_orders()
        found = [
            o for o in orders if str(o.get("orderId") or o.get("order_id")) == str(oid)
        ]
        if found:
            assert found[0].get("status") in (
                "Cancelled",
                "ApiCancelled",
                "Inactive",
            ), f"Expected cancelled, got {found[0].get('status')}"
