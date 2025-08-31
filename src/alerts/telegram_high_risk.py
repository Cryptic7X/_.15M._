"""
High-Risk Telegram Alert System for 15-Minute CipherB Analysis
Optimized for rapid decision-making with essential data
"""

import os
import requests
from datetime import datetime

def send_high_risk_alert(coin_data, signal_type, wt1_val, wt2_val, exchange_used, signal_timestamp):
    """
    Send high-risk 15-minute CipherB alert with essential trading data
    """
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('HIGH_RISK_TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("⚠️ High-risk alert system: Missing Telegram credentials")
        print(f"   Bot token exists: {bool(bot_token)}")
        print(f"   High-risk chat ID exists: {bool(chat_id)}")
        return False
    
    # Extract coin data
    symbol = coin_data.get('symbol', '').upper()
    price = coin_data.get('current_price', 0)
    change_24h = coin_data.get('price_change_percentage_24h', 0)
    market_cap = coin_data.get('market_cap', 0)
    volume = coin_data.get('total_volume', 0)
    
    # Professional price formatting
    if price < 0.001:
        price_formatted = f"${price:.8f}"
    elif price < 0.01:
        price_formatted = f"${price:.6f}"
    elif price < 1:
        price_formatted = f"${price:.4f}"
    else:
        price_formatted = f"${price:.3f}"
    
    # Market data formatting
    market_cap_m = market_cap / 1_000_000
    volume_m = volume / 1_000_000
    
    # Signal formatting
    signal_emoji = "🔔🟢" if signal_type.upper() == "BUY" else "🔔🔴"
    
    # Risk classification based on market cap
    if market_cap >= 1_000_000_000:
        risk_class = "🔷 MID-RISK"
    elif market_cap >= 500_000_000:
        risk_class = "⚡ HIGH-RISK"
    else:
        risk_class = "🔥 EXTREME-RISK"
    
    # Professional 15m chart link
    clean_symbol = symbol.replace('USDT', '').replace('USD', '')
    tv_15m_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=15"
    
    # High-risk alert message for 15m pure CipherB
    message = f"""{signal_emoji} *HIGH-RISK {signal_type.upper()}*

🎯 *{symbol}/USDT* | {risk_class}
📊 *15-MINUTE RAPID ANALYSIS*

💰 *Market Snapshot:*
   • Price: {price_formatted}
   • 24h Change: {change_24h:+.2f}%
   • Market Cap: ${market_cap_m:,.0f}M
   • Volume: ${volume_m:,.0f}M

🌊 *CIPHERB SIGNAL:*
   • WT1: {wt1_val:.1f}
   • WT2: {wt2_val:.1f}
   • Timeframe: 15m
   • Source: {exchange_used}

📈 *QUICK CHART:*
   [TradingView 15m →]({tv_15m_link})

🕐 {datetime.now().strftime('%H:%M:%S IST')} | {signal_timestamp.strftime('%m/%d')}

─────────────────────────────
🔥 *High-Risk 15m System v1.0*
⚡ Rapid Signals | 165 Coins | Pure CipherB"""

    # Send high-risk alert
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
        print(f"🔔 HIGH-RISK alert sent: {symbol} {signal_type}")
        return True
    except requests.RequestException as e:
        print(f"❌ High-risk alert failed for {symbol}: {e}")
        return False
