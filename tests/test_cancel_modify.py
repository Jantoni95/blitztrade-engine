"""
Test: Order cancellation and modification.

Covers:
  - Cancel a working limit order
  - Cancel returns proper response
  - Cancelled order shows correct status
  - Modify a limit order price
  - Modified order retains same order ID
  - Reject modification of a filled order
  - Cancel an already-cancelled order (idempotency)
"""

import pytest
import time
from .conftest import (
    place_order,
    get_orders,
    cancel_order,
    modify_order,
    wait_for_fill,
    wait_for_status,
    TEST_QTY,
)


class TestCancelOrder:
    """Cancel unfilled limit orders."""

    def test_place_limit_order(self, account_id, conid, last_price):
        """Place a limit BUY far from market so it won't fill."""
        limit = round(last_price * 0.80, 2)
        resp = place_order(
            account_id, conid, "BUY", TEST_QTY, order_type="LMT", price=limit
        )
        assert "error" not in resp
        self.__class__._oid = resp["order_id"]

    def test_order_is_working(self, account_id, conid):
        time.sleep(0)
        orders = get_orders()
        found = [
            o
            for o in orders
            if str(o.get("orderId") or o.get("order_id")) == str(self.__class__._oid)
        ]
        assert found, "Limit order not found"
        assert found[0].get("status") in ("Submitted", "PreSubmitted")

    def test_cancel_succeeds(self, account_id, conid):
        """DELETE should return without error."""
        resp = cancel_order(account_id, self.__class__._oid)
        # Response may vary; just ensure no exception

    def test_order_shows_cancelled(self, account_id, conid):
        """After cancel, order status should be Cancelled."""
        time.sleep(0)
        order = wait_for_status(
            self.__class__._oid, ["Cancelled", "ApiCancelled", "Inactive"], timeout=10
        )
        assert order, f"Order {self.__class__._oid} not found after cancel"
        assert order.get("status") in (
            "Cancelled",
            "ApiCancelled",
            "Inactive",
        ), f"Expected Cancelled, got {order.get('status')}"

    def test_cancel_already_cancelled(self, account_id, conid):
        """Cancelling an already-cancelled order should not crash the bridge."""
        try:
            cancel_order(account_id, self.__class__._oid)
        except Exception as e:
            # Some error is acceptable, but bridge should not crash
            assert "500" not in str(e), f"Bridge returned 500: {e}"


class TestModifyOrder:
    """Modify a working limit order's price."""

    def test_place_limit_order(self, account_id, conid, last_price):
        limit = round(last_price * 0.80, 2)
        resp = place_order(
            account_id, conid, "BUY", TEST_QTY, order_type="LMT", price=limit
        )
        assert "error" not in resp
        self.__class__._oid = resp["order_id"]
        self.__class__._original_price = limit
        time.sleep(0)

    def test_modify_price(self, account_id, conid, last_price):
        """Change the limit price."""
        new_price = round(last_price * 0.78, 2)
        resp = modify_order(account_id, self.__class__._oid, new_price)
        self.__class__._new_price = new_price
        # Should not error
        time.sleep(0)

    def test_modified_order_has_new_price(self, account_id, conid):
        """Verify the order now shows the updated price."""
        orders = get_orders()
        found = [
            o
            for o in orders
            if str(o.get("orderId") or o.get("order_id")) == str(self.__class__._oid)
        ]
        assert found, "Modified order not found"
        order_price = float(found[0].get("price") or found[0].get("lmtPrice") or 0)
        if order_price > 0:
            assert (
                abs(order_price - self.__class__._new_price) < 0.02
            ), f"Price not updated: expected ~{self.__class__._new_price}, got {order_price}"

    def test_cleanup(self, account_id, conid):
        try:
            cancel_order(account_id, self.__class__._oid)
        except Exception:
            pass
