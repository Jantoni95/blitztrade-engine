# BlitzTrade Engine

This is the **open-source engine** behind [BlitzTrade](https://blitztrade.io) — a real-time stock screener and trading terminal for Interactive Brokers.

We publish this code so you can see exactly what runs on your computer. No hidden logic, no data collection, no backdoors — just a straightforward bridge between your browser and your IB account.

> **This is not the full application.** The user interface (what you actually see and interact with) is not included here. To use BlitzTrade, download the app from [blitztrade.io](https://blitztrade.io).

---

## What's in this repo?

| File | What it does |
|------|-------------|
| `serve.py` | The core engine — connects to TWS/IB Gateway via the API and serves data to the UI over REST + WebSocket |
| `launcher.py` | Opens the app window on your desktop |
| `build_app.py` | Compiles the engine into the downloadable binary |
| `build_installer.py` | Packages the Windows installer |
| `tests/` | Automated tests for order flow, positions, brackets, etc. |
| `start.sh` / `start.bat` | Start scripts for macOS/Linux and Windows |
| `release_notes/` | Version history with changelogs |

## What's NOT in this repo?

The **user interface** (`index.html`) — the screener, charts, trading panel, alerts, and everything you see on screen. That's the product you get when you download BlitzTrade.

## Why publish this?

**Trust.** BlitzTrade connects to your brokerage account. You're right to want to know what it's doing. This repo lets you (or anyone you trust) read the code that:

- Connects to TWS / IB Gateway
- Places, modifies, and cancels orders
- Streams market data and L2 depth
- Handles positions and P&L
- Builds the binary you download

If something looks off, [open an issue](https://github.com/Jantoni95/blitztrade-engine/issues) or reach out at [blitztrade.io](https://blitztrade.io).

## Publish Log

Every time we release a new version, this repo is updated automatically from our private development repository. See [PUBLISH_LOG.md](PUBLISH_LOG.md) for the full history of what changed and when.

---

**Download BlitzTrade →** [blitztrade.io](https://blitztrade.io)
