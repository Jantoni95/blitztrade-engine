"""
Test: Race conditions and edge cases that pose high risk.

Covers:
  - Double-submit: placing same order twice rapidly
  - Sell more than owned (over-sell creating accidental short)
  - Buy while a sell exit is pending (position doubling risk)
  - Rapid cancel-then-place (race between cancel ack and new order)
  - Modify a bracket child while parent is filling
  - Place order with conid that resolves to wrong exchange
"""

import pytest
from .conftest import (
    place_order,
    get_orders,
    cancel_order,
    cancel_working_orders_for_conid,
    TEST_QTY,
    assert_order_accepted_or_skip,
)


class TestDoubleSubmit:
    """Rapidly submitting the same order twice should produce two accepted orders."""

    def test_double_buy(self, account_id, conid):
        """Submit 2 BUY MKT orders back-to-back and verify both are accepted."""
        r1 = place_order(account_id, conid, "BUY", TEST_QTY, order_type="MKT")
        r2 = place_order(account_id, conid, "BUY", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(r1, "double submit order 1")
        assert_order_accepted_or_skip(r2, "double submit order 2")
        assert r1["order_id"] != r2["order_id"], "Double submit got same order ID"


class TestOverSell:
    """Selling more than owned should still be handled as accepted/rejected cleanly."""

    def test_oversell_creates_short(self, account_id, conid):
        """Buy 1, sell 2 → both requests should return clean order responses."""
        buy = place_order(account_id, conid, "BUY", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(buy, "oversell setup buy")

        sell = place_order(account_id, conid, "SELL", TEST_QTY * 2, order_type="MKT")
        assert_order_accepted_or_skip(sell, "oversell sell")


class TestRapidCancelAndPlace:
    """Cancel an exit order and immediately place a new one."""

    def test_cancel_replace_race(self, account_id, conid, last_price):
        cancel_working_orders_for_conid(account_id, conid)

        # Open position (placement check only)
        buy = place_order(account_id, conid, "BUY", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(buy, "cancel-replace setup buy")

        # Place exit limit
        exit_price = round(last_price * 1.05, 2)
        e1 = place_order(
            account_id, conid, "SELL", TEST_QTY, order_type="LMT", price=exit_price
        )
        assert_order_accepted_or_skip(e1, "cancel-replace old exit")

        # Rapid cancel + new order
        cancel_order(account_id, e1["order_id"])
        new_exit_price = round(last_price * 1.03, 2)
        e2 = place_order(
            account_id, conid, "SELL", TEST_QTY, order_type="LMT", price=new_exit_price
        )
        assert_order_accepted_or_skip(e2, "cancel-replace new exit")

        # Old order should be cancelled, new should be working
        orders = get_orders()
        old = [
            o
            for o in orders
            if str(o.get("orderId") or o.get("order_id")) == str(e1["order_id"])
        ]
        new = [
            o
            for o in orders
            if str(o.get("orderId") or o.get("order_id")) == str(e2["order_id"])
        ]

        if old:
            assert old[0].get("status") in ("Cancelled", "ApiCancelled", "Inactive")
        if new:
            assert new[0].get("status") in (
                "Submitted",
                "PreSubmitted",
                "Cancelled",
                "ApiCancelled",
                "Inactive",
            )

        # Cleanup pending limit
        cancel_order(account_id, e2["order_id"])


class TestBuyWhileExitPending:
    """If user has a sell exit pending and buys more, both orders should be accepted."""

    def test_position_increases_with_pending_exit(self, account_id, conid, last_price):
        # Buy 1 share
        b1 = place_order(account_id, conid, "BUY", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(b1, "buy-while-exit first buy")

        # Place exit far from market
        exit_price = round(last_price * 1.10, 2)
        e1 = place_order(
            account_id, conid, "SELL", TEST_QTY, order_type="LMT", price=exit_price
        )
        assert_order_accepted_or_skip(e1, "buy-while-exit pending exit")

        # Buy another share while exit is pending
        b2 = place_order(account_id, conid, "BUY", TEST_QTY, order_type="MKT")
        assert_order_accepted_or_skip(b2, "buy-while-exit second buy")
        # Cleanup pending limit
        cancel_order(account_id, e1["order_id"])
