#!/usr/bin/env python3
import os
import sys
import time
import json
import ccxt
import pandas as pd
import yaml
from datetime import datetime, timedelta

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

from alerts.telegram_high_risk import send_batched_alert
from alerts.deduplication_15m import AlertDeduplicator
from utils.heikin_ashi import heikin_ashi
from indicators.cipherb_fixed import detect_cipherb_signals

class CipherB15mAnalyzer:
    def __init__(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        
        self.deduplicator = AlertDeduplicator()
        self.exchanges = self.init_exchanges()
        self.market_data = self.load_market_data()

    def load_market_data(self):
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'high_risk_market_data.json')
        if not os.path.exists(cache_file):
            print("❌ Market data not found. Run data fetcher first.")
            return []
        
        with open(cache_file) as f:
            data = json.load(f)
        
        coins = data.get('coins', [])
        print(f"📊 Loaded {len(coins)} coins for analysis")
        return coins

    def init_exchanges(self):
        exchanges = []
        
        # BingX (Primary)
        try:
            bingx = ccxt.bingx({
                'apiKey': os.getenv('BINGX_API_KEY', ''),
                'secret': os.getenv('BINGX_SECRET_KEY', ''),
                'sandbox': False,
                'rateLimit': 300,
                'enableRateLimit': True,
                'timeout': 20000,
            })
            exchanges.append(('BingX', bingx))
        except:
            pass
        
        # KuCoin (Fallback)  
        try:
            kucoin = ccxt.kucoin({
                'rateLimit': 500,
                'enableRateLimit': True,
                'timeout': 20000,
            })
            exchanges.append(('KuCoin', kucoin))
        except:
            pass
        
        print(f"📊 Initialized {len(exchanges)} exchanges")
        return exchanges

    def fetch_15m_data(self, symbol):
        for exchange_name, exchange in self.exchanges:
            try:
                ohlcv = exchange.fetch_ohlcv(f"{symbol}/USDT", '15m', limit=200)
                if len(ohlcv) < 100:
                    continue
                
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                # Convert to IST
                df.index = df.index + pd.Timedelta(hours=5, minutes=30)
                
                return df, exchange_name
            except:
                continue
        return None, None

    def analyze_coin(self, coin_data):
        symbol = coin_data.get('symbol', '').upper()
        
        try:
            # Fetch 15m data
            price_df, exchange_used = self.fetch_15m_data(symbol)
            if price_df is None:
                return None
            
            # Convert to Heikin-Ashi
            ha_data = heikin_ashi(price_df)
            
            # Apply CipherB
            signals = detect_cipherb_signals(ha_data, self.config['cipherb'])
            if signals.empty:
                return None
            
            # Get latest signal
            latest = signals.iloc[-1]
            timestamp = signals.index[-1]
            
            # Check for signals with deduplication
            if latest['buySignal'] and self.deduplicator.is_allowed(symbol, 'BUY', timestamp):
                return {
                    'coin': coin_data,
                    'signal': 'BUY',
                    'wt1': latest['wt1'],
                    'wt2': latest['wt2'],
                    'exchange': exchange_used,
                    'timestamp': timestamp
                }
            elif latest['sellSignal'] and self.deduplicator.is_allowed(symbol, 'SELL', timestamp):
                return {
                    'coin': coin_data,
                    'signal': 'SELL', 
                    'wt1': latest['wt1'],
                    'wt2': latest['wt2'],
                    'exchange': exchange_used,
                    'timestamp': timestamp
                }
            
            return None
            
        except Exception as e:
            return None

    def run_analysis(self):
        from datetime import datetime, timedelta
        ist_time = datetime.utcnow() + timedelta(hours=5, minutes=30)
        print(f"🔔 HIGH-RISK 15M ANALYSIS - {ist_time.strftime('%Y-%m-%d %H:%M:%S IST')}")

        
        if not self.market_data:
            return
        
        signals = []
        batch_size = self.config['alert']['batch_size']
        
        for i in range(0, len(self.market_data), batch_size):
            batch = self.market_data[i:i + batch_size]
            print(f"🔄 Analyzing batch {i//batch_size + 1}")
            
            for coin in batch:
                result = self.analyze_coin(coin)
                if result:
                    signals.append(result)
                    print(f"🔔 {result['signal']}: {coin['symbol'].upper()}")
                
                time.sleep(self.config['exchange']['rate_limit_sec'])
        
        # Send batched alerts
        if signals:
            send_batched_alert(signals)
            print(f"✅ Sent batched alert with {len(signals)} signals")
        else:
            print("📊 No signals found")
        
        print(f"🎯 Analysis complete: {len(signals)} signals from {len(self.market_data)} coins")

if __name__ == '__main__':
    analyzer = CipherB15mAnalyzer()
    analyzer.run_analysis()
