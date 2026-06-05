# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Korean stock trading simulation system that uses Korea Investment & Securities (KIS) API for market data. The system simulates trading strategies with virtual positions, tracking stop-losses, target prices, and volume-based trading decisions without executing real trades.

## Architecture

### Core Components

**Flask Web Server** (`simul_server.py`):
- REST API for managing simulated trades
- Web interface served via `templates/simul.html`
- 17 API endpoints for buy/sell operations, portfolio management, and simulation execution

**Simulation Scripts**:
- `kis_trading_set_simul.py`: Daily position rollover. Reads previous day's `trading_trail_simul` records and creates next-day tracking records. Validates prices against previous day's high/low and updates daily account balance.
- `kis_trading_trail_vol_state_simul.py`: Real-time position monitoring (1550 lines). Tracks stop-loss triggers, target price exits, volume-based state changes, and updates positions throughout trading day. Does NOT execute actual trades.

### Database Schema

PostgreSQL database `fund_risk_mng` on `192.168.50.81:5432`:

**Key Tables**:
- `trading_trail_simul`: Active position tracking with trail_tp states ('1'=new, '2'=tracking, '3'=exited, 'L'=long-term, 'P'=pending, 'C'=closed, 'U'=updated)
- `dly_trading_balance_simul`: Daily per-stock balance aggregation (remaining qty, purchase amount, valuation)
- `dly_acct_balance_simul`: Daily account-level totals (cash, holdings value, realized P&L)
- `stockAccount_stock_account`: KIS API credentials and auth tokens

**Connection String**: `dbname='fund_risk_mng' host='192.168.50.81' port='5432' user='postgres' password='asdf1234'`

### External Integrations

**KIS API** (`https://openapi.koreainvestment.com:9443`):
- OAuth token refresh (daily expiration handling)
- Price data: 1-minute charts, daily OHLC, historical data
- Credentials stored in `stockAccount_stock_account` table per account nickname

**KRX (Korea Exchange)**:
- Stock symbol lookup via `http://kind.krx.co.kr/corpgeneral/corpList.do?method=download`
- Cached in memory (`_krx_df` global)

### Key Constants

- `SIMUL_ACCT = "SIMUL"`: Simulation account identifier in trading_trail_simul
- `SIMUL_DLY_ACCT = "74346047"`: Account ID for dly_acct_balance_simul
- `INITIAL_CAPITAL = 20_000_000`: Starting simulation capital (20M KRW)
- `API_NICK = "phills2"`: Default KIS account nickname for market data queries

## Running the System

### Start Web Server
```bash
python simul_server.py
```
Serves on default Flask port. Access UI at `http://localhost:5000/` (serves `templates/simul.html`).

### Daily Position Setup
```bash
python kis_trading_set_simul.py [YYYYMMDD]
```
Rolls previous day's positions to specified date (defaults to today). Run once per trading day before market open.

### Intraday Position Tracking
```bash
python kis_trading_trail_vol_state_simul.py [YYYYMMDD]
```
Monitors positions and updates stop-loss/target exits. Run during market hours for real-time simulation.

## Development Notes

### Price Validation Logic
- Stop/exit prices > previous low → adjusted to previous low
- Target prices < previous high → adjusted to previous high
- Uses KIS API `FHKST03010100` (daily chart) for validation

### Position State Machine
Positions flow through trail_tp states:
- '1' (new) → '2' (tracking) when volume confirms
- '2' → '3' (exited) on stop-loss or target hit
- '3' → 'L' (long-term hold) if re-entered
- Multiple same-stock positions merge on rollover (aggregate qty, weighted avg price)

### Trade Types (trade_tp)
- 'M': Manual entry (user-initiated via web UI)
- Other types defined in kis_trading_trail_vol_state_simul.py

### Cash Flow Calculations
Daily account balance formula in `kis_trading_set_simul.py:346`:
```
prvs_excc_amt = base_capital - pchs_amt + prev_pchs + total_profit_loss_amt
```
Where base_capital is previous day's cash, pchs_amt is current holdings cost, prev_pchs is previous holdings cost, and total_profit_loss_amt is realized P&L for the day.

### API Rate Limiting
`time.sleep(0.2)` between KIS API calls to avoid throttling (kis_trading_set_simul.py:241).

## Database Stored Procedures

- `post_business_day_char(date)`: Returns next business day
- `prev_business_day_char(date)`: Returns previous business day
- `is_business_day(date)`: Checks if date is trading day

## Web API Key Endpoints

- `POST /api/save`: Insert new position (validates cash balance and market ratio limits)
- `POST /api/sell`: Close position partially/fully
- `POST /api/run-set`: Trigger kis_trading_set_simul.py
- `POST /api/run-trail`: Trigger kis_trading_trail_vol_state_simul.py
- `GET /api/dashboard`: Portfolio summary with P&L
- `GET /api/list`: List active positions for date
- `DELETE /api/delete/<trail_day>/<code>/<trail_tp>`: Remove specific position
