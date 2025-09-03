"""
High-Risk Telegram Alert System - Offset Schedule Compatible
Displays proper IST timestamps in alerts
"""

import os
import requests
from datetime import datetime, timedelta

def get_ist_time():
    """Convert UTC to IST"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)

def send_batched_alert(signals):
    """Send all high-risk signals in consolidated Telegram message"""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('HIGH_RISK_TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id or not signals:
        return False

    # Get current IST time
    ist_time = get_ist_time()
    current_time = ist_time.strftime('%H:%M:%S IST')

    # Group signals by type
    buy_signals = [s for s in signals if s['signal'] == 'BUY']
    sell_signals = [s for s in signals if s['signal'] == 'SELL']

    message = f"""🔔 *HIGH-RISK 15M OFFSET ALERT*
📊 *{len(signals)} SIGNALS DETECTED*
🕐 *{current_time}*
⏰ *Offset Schedule: 3,18,33,48 min/hr*

"""

    if buy_signals:
        message += "🟢 *BUY SIGNALS:*\n"
        for i, signal in enumerate(buy_signals, 1):
            coin = signal['coin']
            symbol = coin['symbol'].upper()
            price = coin.get('current_price', 0)
            change = coin.get('price_change_percentage_24h', 0)
            cap = coin.get('market_cap', 0) / 1_000_000

            # Format price based on value
            if price < 0.001:
                price_fmt = f"${price:.8f}"
            elif price < 0.01:
                price_fmt = f"${price:.6f}"
            elif price < 1:
                price_fmt = f"${price:.4f}"
            else:
                price_fmt = f"${price:.3f}"

            # TradingView link
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=15"

            message += f"""
{i}. *{symbol}* | {price_fmt} | {change:+.1f}%
   Cap: ${cap:.0f}M | WT: {signal['wt1']:.1f}/{signal['wt2']:.1f}
   Ex: {signal['exchange']} | [Chart →]({tv_link})"""

    if sell_signals:
        message += f"\n\n🔴 *SELL SIGNALS:*\n"
        for i, signal in enumerate(sell_signals, 1):
            coin = signal['coin']
            symbol = coin['symbol'].upper()
            price = coin.get('current_price', 0)
            change = coin.get('price_change_percentage_24h', 0)
            cap = coin.get('market_cap', 0) / 1_000_000

            # Format price based on value
            if price < 0.001:
                price_fmt = f"${price:.8f}"
            elif price < 0.01:
                price_fmt = f"${price:.6f}"
            elif price < 1:
                price_fmt = f"${price:.4f}"
            else:
                price_fmt = f"${price:.3f}"

            # TradingView link  
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=15"

            message += f"""
{i}. *{symbol}* | {price_fmt} | {change:+.1f}%
   Cap: ${cap:.0f}M | WT: {signal['wt1']:.1f}/{signal['wt2']:.1f}
   Ex: {signal['exchange']} | [Chart →]({tv_link})"""

    # Footer
    message += f"""

📈 *SUMMARY:*
• Total: {len(signals)} | Buy: {len(buy_signals)} | Sell: {len(sell_signals)}
• Timeframe: 15m | Pure CipherB + Heikin Ashi
• Schedule: Offset mins avoid GitHub rush hours

🔥 *High-Risk Offset System v1.0*"""

    # Send batched alert
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown',
        'disable_web_page_preview': False
    }

    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        print(f"🔔 Offset schedule alert sent: {len(signals)} signals")
        return True
    except Exception as e:
        print(f"❌ Alert failed: {e}")
        return False
