"""Strict fill-sensitive integration checks.

This module is intentionally not named test_*.py, so it is not part of the
default fast integration suite. Run it explicitly when you want to validate
fill metadata and position propagation, not just order acknowledgement.
"""

import pytest

from .conftest import (
    TEST_QTY,
    assert_order_accepted_or_skip,
    cancel_working_orders_for_conid,
    get_marketable_limit_price,
    wait_for_fill_details,
    wait_for_position_visible,
)


pytestmark = pytest.mark.fill_sensitive


class TestStrictLongLifecycle:
    def test_buy_has_fill_price_and_position(self, strict_fill_ctx):
        cancel_working_orders_for_conid(
            strict_fill_ctx.account_id, strict_fill_ctx.conid
        )
        buy_price = get_marketable_limit_price(
            "BUY",
            strict_fill_ctx.conid,
            fallback_price=strict_fill_ctx.last_price,
            account_id=strict_fill_ctx.account_id,
        )
        assert buy_price > 0, "Could not derive marketable BUY limit price"
        resp = strict_fill_ctx.place_order(
            "BUY", TEST_QTY, order_type="LMT", price=buy_price
        )
        assert_order_accepted_or_skip(resp, "strict long buy")

        order = wait_for_fill_details(resp["order_id"])
        assert order is not None, "Order never became visible"
        avg = float(order.get("avgPrice") or order.get("avgFillPrice") or 0)
        assert avg > 0, f"Fill price never populated: {order}"

        pos = wait_for_position_visible(
            strict_fill_ctx.account_id, strict_fill_ctx.conid
        )
        assert pos is not None, "Position never became visible"
        assert pos.get("position", 0) > 0, f"Expected long position, got {pos}"

        sell_price = get_marketable_limit_price(
            "SELL",
            strict_fill_ctx.conid,
            fallback_price=strict_fill_ctx.last_price,
            account_id=strict_fill_ctx.account_id,
        )
        assert sell_price > 0, "Could not derive marketable SELL limit price"
        close = strict_fill_ctx.place_order(
            "SELL", TEST_QTY, order_type="LMT", price=sell_price
        )
        assert_order_accepted_or_skip(close, "strict long cleanup")


class TestStrictShortLifecycle:
    def test_short_has_fill_price_and_negative_position(self, strict_fill_ctx):
        cancel_working_orders_for_conid(
            strict_fill_ctx.account_id, strict_fill_ctx.conid
        )
        short_price = get_marketable_limit_price(
            "SELL",
            strict_fill_ctx.conid,
            fallback_price=strict_fill_ctx.last_price,
            account_id=strict_fill_ctx.account_id,
        )
        assert short_price > 0, "Could not derive marketable short SELL limit price"
        resp = strict_fill_ctx.place_order(
            "SELL", TEST_QTY, order_type="LMT", price=short_price
        )
        assert_order_accepted_or_skip(resp, "strict short sell")

        order = wait_for_fill_details(resp["order_id"])
        assert order is not None, "Short order never became visible"
        avg = float(order.get("avgPrice") or order.get("avgFillPrice") or 0)
        assert avg > 0, f"Short fill price never populated: {order}"

        pos = wait_for_position_visible(
            strict_fill_ctx.account_id, strict_fill_ctx.conid
        )
        assert pos is not None, "Short position never became visible"
        assert pos.get("position", 0) < 0, f"Expected short position, got {pos}"

        cover_price = get_marketable_limit_price(
            "BUY",
            strict_fill_ctx.conid,
            fallback_price=strict_fill_ctx.last_price,
            account_id=strict_fill_ctx.account_id,
        )
        assert cover_price > 0, "Could not derive marketable BUY cover limit price"
        close = strict_fill_ctx.place_order(
            "BUY", TEST_QTY, order_type="LMT", price=cover_price
        )
        assert_order_accepted_or_skip(close, "strict short cleanup")


class TestStrictRoundTrip:
    def test_round_trip_has_fill_prices(self, strict_fill_ctx):
        cancel_working_orders_for_conid(
            strict_fill_ctx.account_id, strict_fill_ctx.conid
        )

        buy_limit = get_marketable_limit_price(
            "BUY",
            strict_fill_ctx.conid,
            fallback_price=strict_fill_ctx.last_price,
            account_id=strict_fill_ctx.account_id,
        )
        assert buy_limit > 0, "Could not derive marketable round-trip BUY limit price"
        buy = strict_fill_ctx.place_order(
            "BUY", TEST_QTY, order_type="LMT", price=buy_limit
        )
        assert_order_accepted_or_skip(buy, "strict round-trip buy")
        buy_order = wait_for_fill_details(buy["order_id"])
        assert buy_order is not None, "Round-trip buy never became visible"
        buy_price = float(
            buy_order.get("avgPrice") or buy_order.get("avgFillPrice") or 0
        )
        assert buy_price > 0, f"Round-trip buy price never populated: {buy_order}"

        sell_limit = get_marketable_limit_price(
            "SELL",
            strict_fill_ctx.conid,
            fallback_price=strict_fill_ctx.last_price,
            account_id=strict_fill_ctx.account_id,
        )
        assert sell_limit > 0, "Could not derive marketable round-trip SELL limit price"
        sell = strict_fill_ctx.place_order(
            "SELL", TEST_QTY, order_type="LMT", price=sell_limit
        )
        assert_order_accepted_or_skip(sell, "strict round-trip sell")
        sell_order = wait_for_fill_details(sell["order_id"])
        assert sell_order is not None, "Round-trip sell never became visible"
        sell_price = float(
            sell_order.get("avgPrice") or sell_order.get("avgFillPrice") or 0
        )
        assert sell_price > 0, f"Round-trip sell price never populated: {sell_order}"
