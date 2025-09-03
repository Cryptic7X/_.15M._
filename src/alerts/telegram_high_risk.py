"""
High-Risk Telegram Alert System for 15-Minute CipherB Analysis
Batched alerts for efficient trading workflow
"""
import os
import requests
from datetime import datetime, timedelta

def send_batched_alert(signals):
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('HIGH_RISK_TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id or not signals:
        return False

    # Convert UTC to IST for display
    ist_time = datetime.utcnow() + timedelta(hours=5, minutes=30)
    current_time = ist_time.strftime('%H:%M:%S IST')

    # Group signals
    buys = [s for s in signals if s['signal'] == 'BUY']
    sells = [s for s in signals if s['signal'] == 'SELL']

    message = f"🔔 *HIGH-RISK 15M BATCH ALERT*\n📊 *{len(signals)} SIGNALS*\n🕐 *{current_time}*\n\n"

    if buys:
        message += "🟢 *BUY SIGNALS:*\n"
        for i, signal in enumerate(buys, 1):
            coin = signal['coin']
            symbol = coin['symbol'].upper()
            price = coin.get('current_price', 0)
            change = coin.get('price_change_percentage_24h', 0)
            cap = coin.get('market_cap', 0) / 1_000_000

            price_fmt = f"${price:.4f}" if price < 1 else f"${price:.2f}"
            tv_link = f"https://www.tradingview.com/chart/?symbol={symbol}USDT&interval=15"

            message += f"\n{i}. *{symbol}* | {price_fmt} | {change:+.1f}%\n"
            message += f"   Cap: ${cap:.0f}M | WT: {signal['wt1']:.1f}/{signal['wt2']:.1f}\n"
            message += f"   [Chart →]({tv_link})"

    if sells:
        message += f"\n\n🔴 *SELL SIGNALS:*\n"
        for i, signal in enumerate(sells, 1):
            coin = signal['coin']
            symbol = coin['symbol'].upper()
            price = coin.get('current_price', 0)
            change = coin.get('price_change_percentage_24h', 0)
            cap = coin.get('market_cap', 0) / 1_000_000

            price_fmt = f"${price:.4f}" if price < 1 else f"${price:.2f}"
            tv_link = f"https://www.tradingview.com/chart/?symbol={symbol}USDT&interval=15"

            message += f"\n{i}. *{symbol}* | {price_fmt} | {change:+.1f}%\n"
            message += f"   Cap: ${cap:.0f}M | WT: {signal['wt1']:.1f}/{signal['wt2']:.1f}\n"
            message += f"   [Chart →]({tv_link})"

    message += f"\n\n📈 *SUMMARY:*\n• Total: {len(signals)} | Buy: {len(buys)} | Sell: {len(sells)}\n"
    message += "• Timeframe: 15m | Pure CipherB\n\n🔥 *High-Risk 15m System*"

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown',
        'disable_web_page_preview': False
    }

    try:
        response = requests.post(url, json=payload, timeout=20)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"Telegram alert failed: {e}")
        return False

