# High-Risk 15-Minute CipherB Trading System

Real-time 15-minute Heikin-Ashi CipherB analysis for high-volatility crypto trading with zero duplicate alerts.

## Features

- ⚡ **15-minute timeframe** analysis
- 🔔 **High-risk Telegram alerts**
- 🎯 **Pure CipherB signals** (no confirmations)
- 🚫 **Zero duplicates** with persistent deduplication
- 📊 **165 quality coins** (≥100M cap, ≥20M volume)
- 🕐 **Real-time execution** 2 minutes after candle close

## Setup

1. **Clone repository**
2. **Install dependencies:** `pip install -r requirements.txt`
3. **Configure Telegram:** Set `HIGH_RISK_TELEGRAM_CHAT_ID` secret
4. **Configure exchanges:** Set BingX API credentials
5. **Run initial scan:** `python src/data_fetcher.py --daily-scan`

## Usage

- **Manual analysis:** `python src/analyzer_15m.py`
- **Automated:** GitHub Actions runs every 15 minutes
- **Market refresh:** Daily at 06:00 IST

## Configuration

Edit `config/config.yaml` to adjust:
- Market cap/volume thresholds
- CipherB parameters
- Alert settings
- Batch sizes

## Alert Format

