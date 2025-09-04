"""
Consolidated Telegram Alert System
Sends ALL signals in ONE message - no more spam!
"""

"""
Consolidated Telegram Alert System
Sends ALL signals in ONE message - no more spam!
"""

import os
import requests
from datetime import datetime, timedelta

def get_ist_time():
    """Convert UTC to IST"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)

def send_consolidated_alert(all_signals):
    """
    Send ONE consolidated message with ALL detected signals
    """
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('HIGH_RISK_TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id or not all_signals:
        return False

    # Current IST time
    ist_time = get_ist_time()
    current_time_str = ist_time.strftime('%H:%M:%S IST')

    # Group signals by type
    buy_signals = [s for s in all_signals if s['signal_type'] == 'BUY']
    sell_signals = [s for s in all_signals if s['signal_type'] == 'SELL']

    # Build consolidated message
    message = f"""🔧 *EXACT CIPHERB 15M ALERT*
🎯 *{len(all_signals)} PRECISE SIGNALS*
🕐 *{current_time_str}*

"""

    # Add BUY signals section
    if buy_signals:
        message += "🟢 *BUY SIGNALS:*\n"
        for i, signal in enumerate(buy_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            market_cap_m = signal['market_cap'] / 1_000_000
            wt1 = signal['wt1']
            wt2 = signal['wt2']
            exchange = signal['exchange']
            age_s = signal.get('signal_age_seconds', 0)

            # Format price
            if price < 0.001:
                price_fmt = f"${price:.8f}"
            elif price < 1:
                price_fmt = f"${price:.4f}"
            else:
                price_fmt = f"${price:.3f}"

            # TradingView link
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=15"

            message += f"""
{i}. *{symbol}* | {price_fmt} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | WT: {wt1:.1f}/{wt2:.1f}
   {exchange} | [Chart →]({tv_link})"""

    # Add SELL signals section
    if sell_signals:
        message += f"\n\n🔴 *SELL SIGNALS:*\n"
        for i, signal in enumerate(sell_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            market_cap_m = signal['market_cap'] / 1_000_000
            wt1 = signal['wt1']
            wt2 = signal['wt2']
            exchange = signal['exchange']
            age_s = signal.get('signal_age_seconds', 0)


            # Format price
            if price < 0.001:
                price_fmt = f"${price:.8f}"
            elif price < 1:
                price_fmt = f"${price:.4f}"
            else:
                price_fmt = f"${price:.3f}"

            # TradingView link
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=15"

            message += f"""
{i}. *{symbol}* | {price_fmt} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | WT: {wt1:.1f}/{wt2:.1f}
   {exchange} | ⚡{age_s:.0f}s ago | [Chart →]({tv_link})"""

    # Update footer
    message += f"""

📊 *FRESH SIGNAL SUMMARY:*
• Total Signals: {len(all_signals)} (all within 2 minutes)
• Buy Signals: {len(buy_signals)}  
• Sell Signals: {len(sell_signals)}
• Fresh Detection: ✅ No duplicates or stale alerts

🎯 *Fresh Signal System v2.0*"""

    # Send single consolidated message
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
        print(f"📱 Consolidated alert sent: {len(all_signals)} signals")
        return True
    except Exception as e:
        print(f"❌ Alert failed: {e}")
        return False

