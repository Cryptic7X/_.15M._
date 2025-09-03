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
    Exactly what you want - single clean alert!
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
    message = f"""🔧 *FIXED 15M BATCH ALERT*
🎯 *{len(all_signals)} PRECISE CROSSOVERS*
🕐 *{current_time_str}*

"""

    # Add BUY signals section
    if buy_signals:
        message += "🟢 *BUY CROSSOVERS:*\n"
        for i, signal in enumerate(buy_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            market_cap_m = signal['market_cap'] / 1_000_000
            wt1 = signal['wt1']
            wt2 = signal['wt2']
            exchange = signal['exchange']

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
        message += f"\n\n🔴 *SELL CROSSOVERS:*\n"
        for i, signal in enumerate(sell_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            market_cap_m = signal['market_cap'] / 1_000_000
            wt1 = signal['wt1']
            wt2 = signal['wt2']
            exchange = signal['exchange']

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

    # Footer
    message += f"""

📊 *CROSSOVER SUMMARY:*
• Total Signals: {len(all_signals)}
• Buy Crossovers: {len(buy_signals)}
• Sell Crossovers: {len(sell_signals)}
• Precise Detection: ✅
• Single Message: ✅

─────────────────────────────
🔧 *Fixed 15m System v1.0*"""

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
        print(f"📱 Consolidated alert sent successfully: {len(all_signals)} signals")
        return True
    except Exception as e:
        print(f"❌ Consolidated alert failed: {e}")
        return False

