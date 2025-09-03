#!/usr/bin/env python3
"""
Fixed Professional 15m Analyzer
- Precise crossover detection (no false signals)
- Batched alerts (single consolidated message)  
- NO Heikin Ashi - Pure OHLCV data only
"""

import os
import sys
import time
import json
import ccxt
import pandas as pd
import yaml
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))

from alerts.telegram_batch import send_consolidated_alert
from alerts.deduplication_fixed import FixedDeduplicator
from indicators.cipherb_exact import detect_exact_cipherb_signals

def get_ist_time():
    """Convert UTC to IST"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)

class Fixed15mAnalyzer:
    def __init__(self):
        self.config = self.load_config()
        self.deduplicator = FixedDeduplicator()
        self.exchanges = self.init_exchanges()
        self.market_data = self.load_market_data()

    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path) as f:
            return yaml.safe_load(f)

    def load_market_data(self):
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'high_risk_market_data.json')
        
        if not os.path.exists(cache_file):
            print("❌ Market data not found")
            return []
        
        with open(cache_file) as f:
            data = json.load(f)
        
        return data.get('coins', [])

    def init_exchanges(self):
        exchanges = []
        
        # BingX Primary
        try:
            bingx = ccxt.bingx({
                'apiKey': os.getenv('BINGX_API_KEY', ''),
                'secret': os.getenv('BINGX_SECRET_KEY', ''),
                'rateLimit': 300,
                'enableRateLimit': True,
                'timeout': 30000,
            })
            exchanges.append(('BingX', bingx))
        except Exception as e:
            print(f"⚠️ BingX failed: {e}")
        
        # KuCoin Fallback
        try:
            kucoin = ccxt.kucoin({
                'rateLimit': 500,
                'enableRateLimit': True,
                'timeout': 30000,
            })
            exchanges.append(('KuCoin', kucoin))
        except Exception as e:
            print(f"⚠️ KuCoin failed: {e}")
        
        return exchanges

    def fetch_15m_ohlcv(self, symbol):
        """Fetch PURE OHLCV data - NO Heikin Ashi conversion"""
        
        for exchange_name, exchange in self.exchanges:
            try:
                ohlcv = exchange.fetch_ohlcv(f"{symbol}/USDT", '15m', limit=200)
                
                if len(ohlcv) < 100:
                    continue
                
                # Pure DataFrame - Direct OHLCV data (NO Heikin Ashi!)
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                # Convert to IST
                df.index = df.index + pd.Timedelta(hours=5, minutes=30)
                
                if len(df) > 50 and df['close'].iloc[-1] > 0:
                    return df, exchange_name
                
            except Exception as e:
                continue
        
        return None, None

    def analyze_coin_precise(self, coin_data):
        symbol = coin_data.get('symbol', '').upper()
        
        try:
            # Fetch pure OHLCV data
            price_df, exchange_used = self.fetch_15m_ohlcv(symbol)
            if price_df is None or len(price_df) < 50:
                return None
            
            # Apply EXACT CipherB detection (matches your Pine Script)
            signals_df = detect_exact_cipherb_signals(price_df, self.config['cipherb'])
            if signals_df.empty:
                return None
            
            # Check only closed candles (second to last)
            if len(signals_df) < 2:
                return None
                
            latest_signal = signals_df.iloc[-2]  # CLOSED candle
            signal_timestamp = signals_df.index[-2]
            
            # Debug logging to compare with TradingView
            if symbol == 'ETH':  # Debug specific coin
                print(f"🔍 {symbol} Debug at {signal_timestamp.strftime('%H:%M')}:")
                print(f"   WT1: {latest_signal['wt1']:.2f}")
                print(f"   WT2: {latest_signal['wt2']:.2f}")
                print(f"   Buy: {latest_signal['buySignal']}")
                print(f"   Sell: {latest_signal['sellSignal']}")
            
            # Check for EXACT Pine Script BUY signal
            if latest_signal['buySignal']:
                if self.deduplicator.is_crossover_allowed(symbol, 'BUY', signal_timestamp):
                    return {
                        'symbol': symbol,
                        'signal_type': 'BUY',
                        'wt1': latest_signal['wt1'],
                        'wt2': latest_signal['wt2'],
                        'price': coin_data.get('current_price', 0),
                        'change_24h': coin_data.get('price_change_percentage_24h', 0),
                        'market_cap': coin_data.get('market_cap', 0),
                        'exchange': exchange_used,
                        'timestamp': signal_timestamp,
                        'coin_data': coin_data
                    }
            
            # Check for EXACT Pine Script SELL signal
            if latest_signal['sellSignal']:
                if self.deduplicator.is_crossover_allowed(symbol, 'SELL', signal_timestamp):
                    return {
                        'symbol': symbol,
                        'signal_type': 'SELL',
                        'wt1': latest_signal['wt1'],
                        'wt2': latest_signal['wt2'],
                        'price': coin_data.get('current_price', 0),
                        'change_24h': coin_data.get('price_change_percentage_24h', 0),
                        'market_cap': coin_data.get('market_cap', 0),
                        'exchange': exchange_used,
                        'timestamp': signal_timestamp,
                        'coin_data': coin_data
                    }
            
            return None
            
        except Exception as e:
            return None

    def run_fixed_analysis(self):
        """
        Main analysis with BATCH collection and SINGLE alert
        """
        ist_current = get_ist_time()
        
        print("="*80)
        print("🔧 FIXED 15M CIPHERB ANALYSIS")
        print("="*80)
        print(f"🕐 IST Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"🎯 Precise Crossover Detection (Pure OHLCV)")
        print(f"📊 Batched Alerts (Single Message)")
        print(f"🔍 Coins to analyze: {len(self.market_data)}")
        
        if not self.market_data:
            print("❌ No market data available")
            return
        
        # COLLECT ALL SIGNALS (Don't send individually)
        all_detected_signals = []
        batch_size = 20
        total_analyzed = 0
        
        for i in range(0, len(self.market_data), batch_size):
            batch = self.market_data[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(self.market_data) - 1) // batch_size + 1
            
            print(f"\n🔄 Processing batch {batch_num}/{total_batches}")
            
            for coin in batch:
                signal_result = self.analyze_coin_precise(coin)
                if signal_result:
                    all_detected_signals.append(signal_result)
                    print(f"🚨 {signal_result['signal_type']}: {signal_result['symbol']}")
                
                total_analyzed += 1
                time.sleep(0.3)  # Rate limiting
        
        # SEND SINGLE CONSOLIDATED ALERT with ALL signals
        if all_detected_signals:
            success = send_consolidated_alert(all_detected_signals)
            if success:
                print(f"\n✅ SENT 1 CONSOLIDATED ALERT with {len(all_detected_signals)} signals")
            else:
                print(f"\n❌ Failed to send consolidated alert")
        else:
            print(f"\n📊 No crossover signals detected in this run")
        
        print(f"\n" + "="*80)
        print("🔧 FIXED ANALYSIS COMPLETE")
        print("="*80)
        print(f"🎯 Total coins analyzed: {total_analyzed}")
        print(f"🚨 Precise signals found: {len(all_detected_signals)}")
        print(f"📱 Telegram messages sent: {'1' if all_detected_signals else '0'}")
        print("="*80)

if __name__ == '__main__':
    analyzer = Fixed15mAnalyzer()
    analyzer.run_fixed_analysis()
