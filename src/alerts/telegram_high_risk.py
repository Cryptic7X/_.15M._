"""
High-Risk Telegram Alert System for 15-Minute CipherB Analysis
Optimized for rapid decision-making with essential data
"""

import os
import requests
from datetime import datetime

def send_batched_high_risk_alert(signals):
    """
    Send all high-risk signals in a single consolidated Telegram message
    """
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('HIGH_RISK_TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("⚠️ Batched alert system: Missing Telegram credentials")
        return False
    
    if not signals:
        return False
    
    # Header
    current_time = datetime.now().strftime('%H:%M:%S IST')
    message = f"""🔔 *HIGH-RISK 15M BATCH ALERT*
📊 *{len(signals)} SIGNALS DETECTED*
🕐 *{current_time}*

"""
    
    # Group signals by type
    buy_signals = [s for s in signals if s['signal_type'] == 'BUY']
    sell_signals = [s for s in signals if s['signal_type'] == 'SELL']
    
    # BUY Signals Section
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
            
            message += f"""
{i}. *{symbol}* | {price_fmt} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | WT: {signal['wt1']:.1f}/{signal['wt2']:.1f}
   [Chart →]({tv_link})"""
    
    # SELL Signals Section
    if sell_signals:
        message += f"\n\n🔴 *SELL SIGNALS:*\n"
        for i, signal in enumerate(sell_signals, 1):
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
            
            message += f"""
{i}. *{symbol}* | {price_fmt} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | WT: {signal['wt1']:.1f}/{signal['wt2']:.1f}
   [Chart →]({tv_link})"""
    
    # Footer
    message += f"""

📈 *SUMMARY:*
• Total Signals: {len(signals)}
• Buy: {len(buy_signals)} | Sell: {len(sell_signals)}
• Timeframe: 15m | Pure CipherB

─────────────────────────────
🔥 *High-Risk 15m System v1.0*"""
    
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
        print(f"🔔 Batched HIGH-RISK alert sent: {len(signals)} signals")
        return True
    except requests.RequestException as e:
        print(f"❌ Batched alert failed: {e}")
        return False

