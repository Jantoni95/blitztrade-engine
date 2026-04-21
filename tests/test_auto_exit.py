"""
Test: Auto-exit trigger behavior — verifying the bridge's bracket/child
order mechanics that the frontend auto-exit feature relies on.

The frontend auto-exit (TP at offset from fill) places orders via the same
bridge endpoint. These tests verify:
  - Entry fill → immediate exit order placement at offset
  - Exit order is on the correct side
  - Exit order at correct price (fill ± offset)
  - Multiple exit levels (stacked TP) each produce separate orders
  - Cancelling an exit order doesn't affect the position
  - Re-arming (placing a new exit after cancelling)
"""

import pytest
import time
from .conftest import (
    place_order,
    get_orders,
    wait_for_fill,
    wait_for_status,
    find_position,
    cancel_order,
    TEST_QTY,
    FILL_TIMEOUT,
    assert_order_accepted_or_skip,
    require_class_attr,
    require_filled,
)


class TestAutoExitLong:
    """Simulate auto-exit for a long position:
    1. BUY at market
    2. Place a LMT SELL at fill_price + offset (simulating frontend auto-exit)
    3. Verify exit order is working at correct price
    4. Cancel exit and close manually
    """

    def test_entry_fill(self, integration_ctx):
        resp = integration_ctx.place_order("BUY", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(resp, "long auto-exit entry")
        order = require_filled(resp["order_id"], "long auto-exit entry")
        avg = float(order.get("avgPrice") or order.get("avgFillPrice") or 0)
        if avg <= 0:
            pytest.skip("Entry fill price not populated yet")
        self.__class__._fill_price = avg

    def test_place_exit_at_offset(self, integration_ctx):
        """Place LMT SELL at fill + $0.20 offset (auto-exit simulation)."""
        offset = 0.20
        fill_price = require_class_attr(self.__class__, "_fill_price")
        exit_price = round(fill_price + offset, 2)
        resp = integration_ctx.place_order(
            "SELL", TEST_QTY, order_type="LMT", price=exit_price
        )
        assert_order_accepted_or_skip(resp, "long auto-exit limit")
        self.__class__._exit_oid = resp["order_id"]
        self.__class__._exit_price = exit_price

    def test_exit_order_is_working(self, integration_ctx):
        time.sleep(0)
        exit_oid = require_class_attr(self.__class__, "_exit_oid")
        orders = get_orders()
        found = [
            o
            for o in orders
            if str(o.get("orderId") or o.get("order_id")) == str(exit_oid)
        ]
        assert found, "Exit order not found"
        assert found[0].get("status") in (
            "Submitted",
            "PreSubmitted",
        ), f"Exit should be working, got {found[0].get('status')}"
        side = (found[0].get("side") or "").upper()
        assert side in ("SELL", "SLD"), f"Auto-exit for long should be SELL, got {side}"

    def test_exit_order_price_correct(self, integration_ctx):
        """The exit order's limit price should match our target."""
        exit_oid = require_class_attr(self.__class__, "_exit_oid")
        exit_price = require_class_attr(self.__class__, "_exit_price")
        orders = get_orders()
        found = [
            o
            for o in orders
            if str(o.get("orderId") or o.get("order_id")) == str(exit_oid)
        ]
        assert found
        order_price = float(found[0].get("price") or found[0].get("lmtPrice") or 0)
        if order_price > 0:
            assert (
                abs(order_price - exit_price) < 0.02
            ), f"Exit price {order_price} != expected {exit_price}"

    def test_cancel_exit_position_intact(self, integration_ctx):
        """Cancelling the exit order should NOT close the position."""
        exit_oid = require_class_attr(self.__class__, "_exit_oid")
        integration_ctx.cancel_order(exit_oid)
        time.sleep(0)
        pos = integration_ctx.find_position()
        assert pos is not None, "Position vanished after cancelling exit order!"
        assert pos.get("position", 0) > 0, "Position should still be long"

    def test_rearm_and_cleanup(self, integration_ctx):
        """Re-arm a new exit, then cancel and close."""
        fill_price = require_class_attr(self.__class__, "_fill_price")
        exit_price = round(fill_price + 0.50, 2)
        resp = integration_ctx.place_order(
            "SELL", TEST_QTY, order_type="LMT", price=exit_price
        )
        assert_order_accepted_or_skip(resp, "long re-arm exit")
        time.sleep(0)
        # Cancel re-armed exit
        integration_ctx.cancel_order(resp["order_id"])
        time.sleep(0)
        # Close at market
        close = integration_ctx.place_order("SELL", TEST_QTY, order_type="MKT")
        if "error" not in close:
            wait_for_fill(close["order_id"])


class TestAutoExitShort:
    """Auto-exit for a short: SELL entry, BUY LMT exit at fill - offset."""

    def test_short_entry(self, integration_ctx):
        resp = integration_ctx.place_order("SELL", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(resp, "short auto-exit entry")
        order = require_filled(resp["order_id"], "short auto-exit entry")
        fill_price = float(order.get("avgPrice") or order.get("avgFillPrice") or 0)
        if fill_price <= 0:
            pytest.skip("Short entry fill price not populated yet")
        self.__class__._fill_price = fill_price

    def test_place_short_exit_at_offset(self, integration_ctx):
        """Place LMT BUY at fill - $0.20 (take profit for short)."""
        fill_price = require_class_attr(self.__class__, "_fill_price")
        exit_price = round(fill_price - 0.20, 2)
        resp = integration_ctx.place_order(
            "BUY", TEST_QTY, order_type="LMT", price=exit_price
        )
        assert_order_accepted_or_skip(resp, "short auto-exit limit")
        self.__class__._exit_oid = resp["order_id"]

    def test_short_exit_is_buy_side(self, integration_ctx):
        time.sleep(0)
        exit_oid = require_class_attr(self.__class__, "_exit_oid")
        orders = get_orders()
        found = [
            o
            for o in orders
            if str(o.get("orderId") or o.get("order_id")) == str(exit_oid)
        ]
        assert found
        side = (found[0].get("side") or "").upper()
        assert side in ("BUY", "BOT"), f"Short auto-exit should be BUY, got {side}"

    def test_cleanup_short_exit(self, integration_ctx):
        exit_oid = require_class_attr(self.__class__, "_exit_oid")
        integration_ctx.cancel_order(exit_oid)
        time.sleep(0)
        close = integration_ctx.place_order("BUY", TEST_QTY, order_type="MKT")
        if "error" not in close:
            wait_for_fill(close["order_id"])


class TestStackedExits:
    """Multiple exit orders at different levels for the same position."""

    def test_entry_and_stacked_exits(self, integration_ctx):
        """Place 2 shares, set 2 separate exit limit orders at different prices."""
        qty = 2
        resp = integration_ctx.place_order("BUY", qty, order_type="MKT")
        assert_order_accepted_or_skip(resp, "stacked entry")
        order = require_filled(resp["order_id"], "stacked entry")
        fill = float(order.get("avgPrice") or order.get("avgFillPrice") or 0)
        if fill <= 0:
            pytest.skip("Stacked entry fill price not populated yet")

        # Exit 1: fill + 0.10 for 1 share
        e1 = integration_ctx.place_order(
            "SELL", 1, order_type="LMT", price=round(fill + 0.10, 2)
        )
        assert_order_accepted_or_skip(e1, "stacked exit 1")
        # Exit 2: fill + 0.30 for 1 share
        e2 = integration_ctx.place_order(
            "SELL", 1, order_type="LMT", price=round(fill + 0.30, 2)
        )
        assert_order_accepted_or_skip(e2, "stacked exit 2")

        self.__class__._exit_oids = [e1["order_id"], e2["order_id"]]
        self.__class__._qty = qty

        time.sleep(0)
        orders = get_orders()
        working = [
            o
            for o in orders
            if str(o.get("orderId") or o.get("order_id"))
            in [str(x) for x in self.__class__._exit_oids]
            and o.get("status") in ("Submitted", "PreSubmitted")
        ]
        assert len(working) == 2, f"Expected 2 working exits, found {len(working)}"

    def test_cleanup_stacked(self, integration_ctx):
        exit_oids = require_class_attr(self.__class__, "_exit_oids")
        qty = require_class_attr(self.__class__, "_qty")
        for oid in exit_oids:
            try:
                integration_ctx.cancel_order(oid)
            except Exception:
                pass
        time.sleep(0)
        close = integration_ctx.place_order("SELL", qty, order_type="MKT")
        if "error" not in close:
            wait_for_fill(close["order_id"])
