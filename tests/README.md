# TWS Bridge Integration Tests

Comprehensive integration tests against the TWS paper trading gateway (port 7497).

## Prerequisites

- TWS/IB Gateway running on port 7497 (paper trading)
- Market hours preferred (some tests need fills)

Note: tests now auto-start a local `serve.py` bridge when `TWS_BRIDGE_URL`
points to localhost and no bridge is already running.

```bash
pip install pytest websockets
```

## Running

```bash
# All tests
pytest tests/ -v --tb=short

# Strict fill-sensitive subset (explicit run)
pytest tests/fill_sensitive_subset.py -m fill_sensitive -v --tb=short

# Specific test file
pytest tests/test_long_position.py -v

# Stop on first failure
pytest tests/ -v -x

# With custom bridge URL
TWS_BRIDGE_URL=http://localhost:8888 pytest tests/ -v
```

## Test Modules

| Module | What it covers |
|--------|---------------|
| `test_connection.py` | Auth, gateway info, WebSocket streaming |
| `test_long_position.py` | Open/close long manually (MKT + LMT) |
| `test_short_position.py` | Open/close short manually, buy-to-cover |
| `test_bracket_orders.py` | TP/SL bracket orders for long and short |
| `test_auto_exit.py` | Simulated auto-exit: entry → exit at offset |
| `test_cancel_modify.py` | Cancel working orders, modify limit price |
| `test_order_validation.py` | Reject bad orders (zero qty, invalid conid, etc.) |
| `test_positions_pnl.py` | Position fields, round-trip P&L, commissions |
| `test_edge_cases.py` | Double-submit, over-sell, race conditions |
| `fill_sensitive_subset.py` | Explicit strict fill/position propagation checks |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TWS_BRIDGE_URL` | `http://localhost:8888` | Bridge base URL |
| `TWS_BRIDGE_AUTOSTART` | `1` | Auto-start local bridge lifecycle in tests (`0` to disable) |
| `TWS_TEST_SYMBOL` | `AAPL` | Preferred symbol used to resolve test conid |
| `TWS_TEST_TWS_PORT` | `7497` | TWS/IB port passed to auto-started `serve.py` |
| `TWS_TEST_CONID` | `756733` | Default conid fallback when symbol/scanner lookup fails |
| `TWS_TEST_USE_SCANNER_CONID` | `0` | Use scanner-selected conid (most-active) |
| `STRICT_FILL_SCAN_CODE` | `TOP_PERC_GAIN` | Scanner code for strict fill-sensitive subset |
| `STRICT_FILL_SCAN_LOCATION` | `STK.US.MAJOR` | Scanner location for strict fill-sensitive subset |
| `STRICT_FILL_MIN_VOLUME` | `500000` | Minimum volume filter for strict fill-sensitive subset |
| `FILL_TIMEOUT` | `15` | Seconds to wait for order fills |
| `FILL_DETAILS_TIMEOUT` | `15` | Seconds to wait for avg fill price metadata |
| `POSITION_TIMEOUT` | `10` | Seconds to wait for position propagation |
| `TEST_QTY` | `1` | Shares per test order |

## Safety

- All tests assert the account contains "D" (paper account check)
- Every test cleans up its positions in teardown
- Default quantity is 1 share to minimize paper impact
