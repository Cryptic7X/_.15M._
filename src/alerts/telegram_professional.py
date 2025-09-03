"""
Professional Telegram Alert System 
Individual alerts like your working 2h system (not batched)
"""

import os
import requests
from datetime import datetime, timedelta

def get_ist_time():
    """Convert UTC to IST"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)

def send_professional_alert(signal_data):
    """
    Send individual professional alert
    Same format as your working 2h system
    """
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('HIGH_RISK_TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("⚠️ Telegram credentials missing")
        return False

    # Get IST time for display
    ist_time = get_ist_time()
    current_time = ist_time.strftime('%H:%M:%S IST')

    symbol = signal_data['symbol']
    signal_type = signal_data['signal']
    price = signal_data['price']
    change_24h = signal_data['change_24h']
    market_cap = signal_data['market_cap'] / 1_000_000  # Convert to millions
    wt1 = signal_data['wt1']
    wt2 = signal_data['wt2']
    exchange = signal_data['exchange']

    # Format price based on value
    if price < 0.001:
        price_fmt = f"${price:.8f}"
    elif price < 0.01:
        price_fmt = f"${price:.6f}"
    elif price < 1:
        price_fmt = f"${price:.4f}"
    else:
        price_fmt = f"${price:.3f}"

    # Signal emoji
    emoji = "🟢" if signal_type == 'BUY' else "🔴"
    
    # TradingView chart link
    clean_symbol = symbol.replace('USDT', '').replace('USD', '')
    tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=15"

    # Professional alert message (same style as 2h system)
    message = f"""{emoji} *PROFESSIONAL 15M ALERT*

🎯 *{symbol} {signal_type}*
💰 Price: {price_fmt} | {change_24h:+.1f}%
📊 Market Cap: ${market_cap:.0f}M
📈 CipherB: WT1={wt1:.1f} | WT2={wt2:.1f}
🔄 Exchange: {exchange}
🕐 Time: {current_time}

[📊 TradingView Chart →]({tv_link})

─────────────────────
🔥 *Professional 15m System*"""

    # Send alert
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
        print(f"📲 Professional alert sent: {symbol} {signal_type}")
        return True
    except Exception as e:
        print(f"❌ Alert failed: {e}")
        return False
