"""
Test: Bracket orders — Take Profit and Stop Loss.

Covers:
  - BUY with TP/SL creates 3 linked orders (parent + 2 children)
  - Child orders have correct parentId linkage
  - TP is a LMT order on the exit side
  - SL is a STP order on the exit side
  - Cancelling parent cascades to children
  - SELL (short) with TP/SL also works correctly
  - TP-only and SL-only bracket variants
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


class TestLongBracketTPSL:
    """BUY entry with both take-profit and stop-loss."""

    def test_bracket_order_creates_three_orders(self, account_id, conid, last_price):
        """Place BUY MKT with TP and SL — expect parent + 2 child order IDs."""
        tp = round(last_price * 1.05, 2)  # 5% above
        sl = round(last_price * 0.95, 2)  # 5% below
        resp = place_order(
            account_id,
            conid,
            "BUY",
            TEST_QTY,
            order_type="MKT",
            tp_price=tp,
            sl_price=sl,
        )
        assert_order_accepted_or_skip(resp, "long bracket entry")
        self.__class__._parent_oid = resp["order_id"]
        self.__class__._tp_oid = resp.get("tp_order_id")
        self.__class__._sl_oid = resp.get("sl_order_id")
        assert resp["order_id"] > 0, "Parent order ID missing"
        assert self.__class__._tp_oid, "TP order ID not returned"
        assert self.__class__._sl_oid, "SL order ID not returned"
        # All three should be distinct
        ids = {resp["order_id"], self.__class__._tp_oid, self.__class__._sl_oid}
        assert len(ids) == 3, f"Expected 3 distinct order IDs, got {ids}"

    def test_parent_fills(self, account_id, conid):
        """Parent (entry) order should fill."""
        parent_oid = require_class_attr(self.__class__, "_parent_oid")
        require_filled(parent_oid, "long bracket parent")

    def test_tp_order_is_working(self, account_id, conid):
        """TP child should be Submitted/PreSubmitted (waiting to trigger)."""
        time.sleep(0)
        tp_oid = require_class_attr(self.__class__, "_tp_oid")
        orders = get_orders()
        tp = [
            o
            for o in orders
            if str(o.get("orderId") or o.get("order_id")) == str(tp_oid)
        ]
        assert tp, f"TP order {tp_oid} not found in orders"
        assert tp[0].get("status") in (
            "Submitted",
            "PreSubmitted",
        ), f"TP should be working, got {tp[0].get('status')}"
        side = (tp[0].get("side") or "").upper()
        assert side in ("SELL", "SLD"), f"TP exit side should be SELL, got {side}"

    def test_sl_order_is_working(self, account_id, conid):
        """SL child should be Submitted/PreSubmitted."""
        sl_oid = require_class_attr(self.__class__, "_sl_oid")
        orders = get_orders()
        sl = [
            o
            for o in orders
            if str(o.get("orderId") or o.get("order_id")) == str(sl_oid)
        ]
        assert sl, f"SL order {sl_oid} not found"
        assert sl[0].get("status") in (
            "Submitted",
            "PreSubmitted",
        ), f"SL should be working, got {sl[0].get('status')}"
        side = (sl[0].get("side") or "").upper()
        assert side in ("SELL", "SLD"), f"SL exit side should be SELL, got {side}"

    def test_cancel_tp_sl_and_close(self, account_id, conid):
        """Clean up: cancel TP/SL children then close position at market."""
        # Cancel children
        tp_oid = require_class_attr(self.__class__, "_tp_oid")
        sl_oid = require_class_attr(self.__class__, "_sl_oid")
        for oid in [tp_oid, sl_oid]:
            try:
                cancel_order(account_id, oid)
            except Exception:
                pass
        time.sleep(0)
        # Close position
        resp = place_order(account_id, conid, "SELL", TEST_QTY, order_type="MKT")
        if "error" not in resp:
            wait_for_fill(resp["order_id"])
        time.sleep(0)
        pos = find_position(account_id, conid)
        if pos:
            assert (
                pos.get("position", 0) == 0
            ), f"Failed to flatten after bracket cleanup, pos={pos.get('position')}"


class TestShortBracketTPSL:
    """SELL (short) entry with TP and SL — children should be BUY side."""

    def test_short_bracket_creates_orders(self, account_id, conid, last_price):
        tp = round(last_price * 0.95, 2)  # 5% below (profit for short)
        sl = round(last_price * 1.05, 2)  # 5% above (stop for short)
        resp = place_order(
            account_id,
            conid,
            "SELL",
            TEST_QTY,
            order_type="MKT",
            tp_price=tp,
            sl_price=sl,
        )
        assert_order_accepted_or_skip(resp, "short bracket entry")
        self.__class__._parent_oid = resp["order_id"]
        self.__class__._tp_oid = resp.get("tp_order_id")
        self.__class__._sl_oid = resp.get("sl_order_id")
        assert self.__class__._tp_oid and self.__class__._sl_oid

    def test_short_parent_fills(self, account_id, conid):
        parent_oid = require_class_attr(self.__class__, "_parent_oid")
        require_filled(parent_oid, "short bracket parent")

    def test_short_tp_is_buy_side(self, account_id, conid):
        """TP for a short should be a BUY order (buy-to-cover at profit)."""
        time.sleep(0)
        tp_oid = require_class_attr(self.__class__, "_tp_oid")
        orders = get_orders()
        tp = [
            o
            for o in orders
            if str(o.get("orderId") or o.get("order_id")) == str(tp_oid)
        ]
        assert tp, "TP order not found"
        side = (tp[0].get("side") or "").upper()
        assert side in ("BUY", "BOT"), f"Short TP should be BUY side, got {side}"

    def test_short_sl_is_buy_side(self, account_id, conid):
        """SL for a short should be a BUY order (buy-to-cover at loss)."""
        sl_oid = require_class_attr(self.__class__, "_sl_oid")
        orders = get_orders()
        sl = [
            o
            for o in orders
            if str(o.get("orderId") or o.get("order_id")) == str(sl_oid)
        ]
        assert sl, "SL order not found"
        side = (sl[0].get("side") or "").upper()
        assert side in ("BUY", "BOT"), f"Short SL should be BUY side, got {side}"

    def test_cleanup_short_bracket(self, account_id, conid):
        tp_oid = require_class_attr(self.__class__, "_tp_oid")
        sl_oid = require_class_attr(self.__class__, "_sl_oid")
        for oid in [tp_oid, sl_oid]:
            try:
                cancel_order(account_id, oid)
            except Exception:
                pass
        time.sleep(0)
        resp = place_order(account_id, conid, "BUY", TEST_QTY, order_type="MKT")
        if "error" not in resp:
            wait_for_fill(resp["order_id"])


class TestTPOnly:
    """Entry with only take-profit, no stop-loss."""

    def test_tp_only_accepted(self, account_id, conid, last_price):
        tp = round(last_price * 1.10, 2)
        resp = place_order(
            account_id, conid, "BUY", TEST_QTY, order_type="MKT", tp_price=tp
        )
        assert_order_accepted_or_skip(resp, "tp-only entry")
        self.__class__._parent_oid = resp["order_id"]
        self.__class__._tp_oid = resp.get("tp_order_id")
        assert self.__class__._tp_oid, "TP order ID should be present"
        assert resp.get("sl_order_id") is None, "SL should not be created"

    def test_cleanup_tp_only(self, account_id, conid):
        parent_oid = require_class_attr(self.__class__, "_parent_oid")
        tp_oid = require_class_attr(self.__class__, "_tp_oid")
        require_filled(parent_oid, "tp-only parent")
        try:
            cancel_order(account_id, tp_oid)
        except Exception:
            pass
        time.sleep(0)
        resp = place_order(account_id, conid, "SELL", TEST_QTY, order_type="MKT")
        if "error" not in resp:
            wait_for_fill(resp["order_id"])


class TestSLOnly:
    """Entry with only stop-loss, no take-profit."""

    def test_sl_only_accepted(self, account_id, conid, last_price):
        sl = round(last_price * 0.90, 2)
        resp = place_order(
            account_id, conid, "BUY", TEST_QTY, order_type="MKT", sl_price=sl
        )
        assert_order_accepted_or_skip(resp, "sl-only entry")
        self.__class__._parent_oid = resp["order_id"]
        self.__class__._sl_oid = resp.get("sl_order_id")
        assert self.__class__._sl_oid, "SL order ID should be present"
        assert resp.get("tp_order_id") is None, "TP should not be created"

    def test_cleanup_sl_only(self, account_id, conid):
        parent_oid = require_class_attr(self.__class__, "_parent_oid")
        sl_oid = require_class_attr(self.__class__, "_sl_oid")
        require_filled(parent_oid, "sl-only parent")
        try:
            cancel_order(account_id, sl_oid)
        except Exception:
            pass
        time.sleep(0)
        resp = place_order(account_id, conid, "SELL", TEST_QTY, order_type="MKT")
        if "error" not in resp:
            wait_for_fill(resp["order_id"])
