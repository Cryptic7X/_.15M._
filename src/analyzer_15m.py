#!/usr/bin/env python3
"""
High-Risk 15-Minute CipherB Analysis Engine
Real-time 15m Heikin-Ashi CipherB analysis with duplicate prevention
"""

#!/usr/bin/env python3
"""
Fixed High-Risk 15-Minute CipherB Analysis Engine
Perfect timing with zero alignment issues
"""

import json
import os
import sys
import time
import ccxt
import pandas as pd
import yaml
from datetime import datetime, timedelta

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from utils.heikin_ashi import heikin_ashi
from utils.timestamp_utils import get_15m_candle_boundaries, format_ist_timestamp, should_run_analysis_now, get_current_analysis_candle
from indicators.cipherb_fixed import detect_cipherb_signals
from alerts.deduplication_15m import HighRisk15mDeduplicator
from alerts.telegram_high_risk import send_batched_high_risk_alert

class HighRisk15mAnalyzer:
    def __init__(self):
        self.config = self.load_configuration()
        self.exchanges = self.initialize_exchanges()
        self.deduplicator = HighRisk15mDeduplicator()
        
    def load_configuration(self):
        """Load system configuration"""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def load_high_risk_market_data(self):
        """Load market data"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'high_risk_market_data.json')
        
        if not os.path.exists(cache_file):
            print("❌ Market data not found. Run data fetcher first.")
            return None
        
        with open(cache_file, 'r') as f:
            data = json.load(f)
        
        coins = data.get('coins', [])
        print(f"📊 Loaded {len(coins)} high-risk coins")
        return coins
    
    def initialize_exchanges(self):
        """Initialize exchanges"""
        exchanges = []
        
        try:
            bingx = ccxt.bingx({
                'apiKey': os.getenv('BINGX_API_KEY', ''),
                'secret': os.getenv('BINGX_SECRET_KEY', ''),
                'rateLimit': 200,
                'enableRateLimit': True,
                'timeout': 25000,
            })
            exchanges.append(('BingX', bingx))
        except Exception as e:
            print(f"⚠️ BingX failed: {e}")
        
        try:
            kucoin = ccxt.kucoin({
                'rateLimit': 800,
                'enableRateLimit': True,
                'timeout': 25000,
            })
            exchanges.append(('KuCoin', kucoin))
        except Exception as e:
            print(f"⚠️ KuCoin failed: {e}")
        
        return exchanges
    
    def fetch_15m_data(self, symbol):
        """Fetch 15m data with proper validation"""
        for exchange_name, exchange in self.exchanges:
            try:
                ohlcv = exchange.fetch_ohlcv(f"{symbol}/USDT", '15m', limit=200)
                
                if len(ohlcv) < 100:
                    continue
                
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                # Keep in UTC - don't convert to IST here
                # IST conversion handled by timestamp_utils
                
                if self.validate_data_quality(df, symbol):
                    return df, exchange_name
                
            except Exception as e:
                continue
        
        return None, None
    
    def validate_data_quality(self, df, symbol):
        """Validate data quality"""
        if df.empty or len(df) < 50:
            return False
        
        if df['Close'].iloc[-1] <= 0 or df['Volume'].iloc[-1] <= 0:
            return False
        
        price_range = df['High'].max() - df['Low'].min()
        if price_range / df['Close'].mean() < 0.005:
            return False
        
        return True
    
    def analyze_coin_15m(self, coin_data):
        """Analyze coin with perfect timing"""
        symbol = coin_data.get('symbol', '').upper()
        
        try:
            # Fetch 15m data
            price_df, exchange_used = self.fetch_15m_data(symbol)
            if price_df is None:
                return None
            
            # Convert to Heikin-Ashi
            ha_data = heikin_ashi(price_df)
            
            # Apply CipherB
            cipherb_signals = detect_cipherb_signals(ha_data, self.config['cipherb'])
            if cipherb_signals.empty:
                return None
            
            # Get latest closed candle signal
            latest_signals = cipherb_signals.iloc[-2]  # Use -2 for fully closed candle
            latest_timestamp = cipherb_signals.index[-2]   # Use -2 for fully closed candle
            
            # Get candle boundaries for this signal
            boundaries = get_15m_candle_boundaries(latest_timestamp)
            
            # Check BUY signal
            if latest_signals['buySignal']:
                if self.deduplicator.is_alert_allowed(symbol, 'BUY', latest_timestamp):
                    return {
                        'coin_data': coin_data,
                        'signal_type': 'BUY',
                        'wt1': latest_signals['wt1'],
                        'wt2': latest_signals['wt2'],
                        'exchange': exchange_used,
                        'timestamp': latest_timestamp,
                        'candle_close_ist': boundaries['candle_close_ist']
                    }
            
            # Check SELL signal
            if latest_signals['sellSignal']:
                if self.deduplicator.is_alert_allowed(symbol, 'SELL', latest_timestamp):
                    return {
                        'coin_data': coin_data,
                        'signal_type': 'SELL',
                        'wt1': latest_signals['wt1'],
                        'wt2': latest_signals['wt2'],
                        'exchange': exchange_used,
                        'timestamp': latest_timestamp,
                        'candle_close_ist': boundaries['candle_close_ist']
                    }
            
            return None
            
        except Exception as e:
            print(f"❌ {symbol} analysis failed: {str(e)[:50]}")
            return None
    
    def run_high_risk_analysis(self):
        """Execute analysis with FIXED timing"""
        
        print(f"\n" + "="*80)
        print("🔔 HIGH-RISK 15M CIPHERB ANALYSIS STARTING")
        print("="*80)
        
        # Use FIXED timestamp functions
        current_ist = get_current_ist_time()
        print(f"🕐 Current IST time: {format_ist_timestamp(current_ist)}")
        
        # Check if we should run now
        if not should_run_analysis_now():
            print("⚠️ Analysis skipped - not the right time to run")
            return
        
        # Get the candle to analyze
        current_candle = get_current_analysis_candle()
        print(f"📊 Analyzing candle: {format_ist_timestamp(current_candle['candle_start_ist'], False)} - {format_ist_timestamp(current_candle['candle_close_ist'], False)}")
        
        # Load market data
        coins = self.load_high_risk_market_data()
        if not coins:
            return
        
        print(f"📈 Coins to analyze: {len(coins)}")
        
        # Collect signals
        all_signals = []
        total_analyzed = 0
        
        for i in range(0, len(coins), self.config['alerts']['batch_size']):
            batch = coins[i:i+self.config['alerts']['batch_size']]
            
            for coin in batch:
                signal = self.analyze_coin_15m(coin)
                if signal:
                    all_signals.append(signal)
                    print(f"🔔 {signal['signal_type']}: {signal['coin_data']['symbol'].upper()}")
                total_analyzed += 1
                time.sleep(self.config['exchanges']['rate_limit'])
        
        # Send batched alert
        if all_signals:
            send_batched_high_risk_alert(all_signals)
            print(f"\n✅ Sent batched alert with {len(all_signals)} signals")
        else:
            print(f"\n📊 No signals found")
        
        print(f"\n" + "="*80)
        print("🔔 ANALYSIS COMPLETE")
        print("="*80)
        print(f"🎯 Coins analyzed: {total_analyzed}")
        print(f"🚨 Signals sent: {len(all_signals)}")
        print("="*80)
        
        # Cleanup
        self.deduplicator.cleanup_expired_entries()

def main():
    analyzer = HighRisk15mAnalyzer()
    analyzer.run_high_risk_analysis()

if __name__ == '__main__':
    main()
