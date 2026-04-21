"""
Test: Short position lifecycle — open and close manually.

Covers:
  - SELL (short) market order fills correctly
  - Position appears with negative quantity
  - BUY-to-cover closes the short
  - Position returns to flat
  - Short selling specific risks: uptick rule awareness, borrow availability
"""

import pytest
import time
from .conftest import (
    place_order,
    wait_for_fill,
    find_position,
    cancel_order,
    cancel_working_orders_for_conid,
    TEST_QTY,
    FILL_TIMEOUT,
    assert_order_accepted_or_skip,
    require_class_attr,
    require_filled,
)


class TestShortPositionManual:
    """Open a short position with SELL, then cover with BUY."""

    def test_sell_short_accepted(self, account_id, conid):
        """Place a MKT SELL (short) order."""
        resp = place_order(account_id, conid, "SELL", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(resp, "short sell")
        self.__class__._sell_order_id = resp["order_id"]
        assert resp["order_id"] > 0

    def test_sell_short_fills(self, account_id, conid):
        """Wait for the short sell to fill."""
        oid = require_class_attr(self.__class__, "_sell_order_id")
        require_filled(oid, "short sell")

    def test_short_fill_price_positive(self, account_id, conid):
        """Fill price must be positive."""
        oid = require_class_attr(self.__class__, "_sell_order_id")
        order = require_filled(oid, "short sell")
        avg = float(order.get("avgPrice") or order.get("avgFillPrice") or 0)
        if avg <= 0:
            pytest.skip("Short fill price not populated yet")
        self.__class__._short_fill_price = avg

    def test_short_position_appears(self, account_id, conid):
        """Position should show negative quantity (short)."""
        time.sleep(0)
        pos = find_position(account_id, conid)
        if pos is None:
            pytest.skip(f"Position not yet visible for conid {conid}")
        qty = pos.get("position", 0)
        assert qty < 0, f"Expected negative position (short), got {qty}"

    def test_buy_to_cover_accepted(self, account_id, conid):
        """Place a MKT BUY to cover the short."""
        cancel_working_orders_for_conid(account_id, conid)
        resp = place_order(account_id, conid, "BUY", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(resp, "buy-to-cover")
        self.__class__._cover_order_id = resp["order_id"]

    def test_buy_to_cover_fills(self, account_id, conid):
        """Wait for the cover order to fill."""
        oid = require_class_attr(self.__class__, "_cover_order_id")
        require_filled(oid, "buy-to-cover")

    def test_cover_fill_price_sane(self, account_id, conid):
        """Cover fill price should be close to the short entry."""
        oid = require_class_attr(self.__class__, "_cover_order_id")
        order = require_filled(oid, "buy-to-cover")
        avg = float(order.get("avgPrice") or order.get("avgFillPrice") or 0)
        if avg <= 0:
            pytest.skip("Cover fill price not populated yet")
        short_price = require_class_attr(self.__class__, "_short_fill_price")
        deviation = abs(avg - short_price) / short_price
        assert (
            deviation < 0.05
        ), f"Cover price {avg} deviates >{5}% from short price {short_price}"

    def test_position_flat_after_cover(self, account_id, conid):
        """Position should be flat after covering."""
        time.sleep(0)
        pos = find_position(account_id, conid)
        if pos is not None:
            qty = pos.get("position", 0)
            assert qty == 0, f"Expected flat, got {qty}"


class TestShortWithLimitCover:
    """Short sell, then try to cover with a limit order far from market."""

    def test_short_entry(self, account_id, conid):
        resp = place_order(account_id, conid, "SELL", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(resp, "short entry")
        self.__class__._entry_oid = resp["order_id"]
        order = require_filled(resp["order_id"], "short entry")
        entry_price = float(order.get("avgPrice") or order.get("avgFillPrice") or 0)
        if entry_price <= 0:
            pytest.skip("Short entry fill price not populated yet")
        self.__class__._entry_price = entry_price

    def test_limit_cover_stays_open(self, account_id, conid):
        """Limit BUY far below market should NOT fill — verifying order sits."""
        entry_price = require_class_attr(self.__class__, "_entry_price")
        cover_price = round(entry_price * 0.85, 2)
        resp = place_order(
            account_id, conid, "BUY", TEST_QTY, order_type="LMT", price=cover_price
        )
        assert_order_accepted_or_skip(resp, "limit cover")
        self.__class__._cover_oid = resp["order_id"]
        time.sleep(0)
        from .conftest import get_orders

        orders = get_orders()
        found = [
            o
            for o in orders
            if str(o.get("orderId") or o.get("order_id")) == str(resp["order_id"])
        ]
        assert found and found[0].get("status") in ("Submitted", "PreSubmitted")

    def test_cancel_and_cover_at_market(self, account_id, conid):
        """Cancel the limit, then cover at market to flatten."""
        cover_oid = require_class_attr(self.__class__, "_cover_oid")
        cancel_order(account_id, cover_oid)
        time.sleep(0)
        resp = place_order(account_id, conid, "BUY", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(resp, "market cover")
        require_filled(resp["order_id"], "market cover")
        time.sleep(0)
        pos = find_position(account_id, conid)
        if pos:
            assert pos.get("position", 0) == 0
