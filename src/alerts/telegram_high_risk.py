"""
High-Risk Telegram Alert System for 15-Minute CipherB Analysis
Batched alerts for efficient trading workflow
"""

"""
Fixed High-Risk Telegram Alerts with Perfect IST Timestamps
"""

import os
import requests
import sys
from datetime import datetime, timedelta
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from timestamp_utils import format_ist_timestamp

def send_batched_high_risk_alert(signals):
    """Send batched alert with correct IST timestamps"""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('HIGH_RISK_TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("⚠️ Missing Telegram credentials")
        return False
    
    if not signals:
        return False
    
    # Use IST time for message header
    current_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    current_time_str = current_ist.strftime('%H:%M:%S IST')
    
    message = f"""🔔 *HIGH-RISK 15M BATCH ALERT*
📊 *{len(signals)} SIGNALS DETECTED*
🕐 *{current_time_str}*

"""
    
    # Group by signal type
    buy_signals = [s for s in signals if s['signal_type'] == 'BUY']
    sell_signals = [s for s in signals if s['signal_type'] == 'SELL']
    
    # BUY Signals
    if buy_signals:
        message += "🟢 *BUY SIGNALS:*\n"
        for i, signal in enumerate(buy_signals, 1):
            coin_data = signal['coin_data']
            symbol = coin_data.get('symbol', '').upper()
            price = coin_data.get('current_price', 0)
            change_24h = coin_data.get('price_change_percentage_24h', 0)
            market_cap_m = coin_data.get('market_cap', 0) / 1_000_000
            
            # Format price
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
            
            # Use correct candle close time
            candle_time = signal['candle_close_ist'].strftime('%H:%M')
            
            message += f"""
{i}. *{symbol}* | {price_fmt} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | WT: {signal['wt1']:.1f}/{signal['wt2']:.1f}
   Candle: {candle_time} IST | [Chart →]({tv_link})"""
    
    # SELL Signals
    if sell_signals:
        message += f"\n\n🔴 *SELL SIGNALS:*\n"
        for i, signal in enumerate(sell_signals, 1):
            coin_data = signal['coin_data']
            symbol = coin_data.get('symbol', '').upper()
            price = coin_data.get('current_price', 0)
            change_24h = coin_data.get('price_change_percentage_24h', 0)
            market_cap_m = coin_data.get('market_cap', 0) / 1_000_000
            
            if price < 0.001:
                price_fmt = f"${price:.8f}"
            elif price < 0.01:
                price_fmt = f"${price:.6f}"
            elif price < 1:
                price_fmt = f"${price:.4f}"
            else:
                price_fmt = f"${price:.3f}"
            
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=15"
            
            candle_time = signal['candle_close_ist'].strftime('%H:%M')
            
            message += f"""
{i}. *{symbol}* | {price_fmt} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | WT: {signal['wt1']:.1f}/{signal['wt2']:.1f}
   Candle: {candle_time} IST | [Chart →]({tv_link})"""
    
    # Footer
    message += f"""

📈 *SUMMARY:*
• Total Signals: {len(signals)}
• Buy: {len(buy_signals)} | Sell: {len(sell_signals)}
• Timeframe: 15m | Pure CipherB

─────────────────────────────
🔥 *High-Risk 15m System v1.0*"""

    # Send alert
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
        print(f"🔔 Batched alert sent: {len(signals)} signals")
        return True
    except requests.RequestException as e:
        print(f"❌ Alert failed: {e}")
        return False
