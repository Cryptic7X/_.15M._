#!/usr/bin/env python3
"""
High-Risk 15-Minute CipherB Analysis Engine
Offset scheduling: runs at 3,18,33,48 minutes past each hour
"""

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

def get_ist_time():
    """Convert UTC to IST for proper timezone display"""
    utc_now = datetime.utcnow()
    ist_time = utc_now + timedelta(hours=5, minutes=30)
    return ist_time

class HighRisk15mAnalyzer:
    def __init__(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        
        self.deduplicator = AlertDeduplicator()
        self.exchanges = self.init_exchanges()
        self.market_data = self.load_market_data()

    def load_market_data(self):
        """Load high-risk filtered market data"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'high_risk_market_data.json')
        
        if not os.path.exists(cache_file):
            print("❌ High-risk market data not found. Run data fetcher first.")
            return []
        
        with open(cache_file) as f:
            data = json.load(f)
        
        coins = data.get('coins', [])
        metadata = data.get('metadata', {})
        
        print(f"📊 Loaded {len(coins)} high-risk coins")
        print(f"🕐 Cache updated: {metadata.get('last_updated', 'Unknown')}")
        
        return coins

    def init_exchanges(self):
        """Initialize exchange connections for 15m data"""
        exchanges = []
        
        # BingX (Primary)
        try:
            bingx_config = {
                'apiKey': os.getenv('BINGX_API_KEY', ''),
                'secret': os.getenv('BINGX_SECRET_KEY', ''),
                'sandbox': False,
                'rateLimit': 300,
                'enableRateLimit': True,
                'timeout': 25000,
            }
            
            bingx = ccxt.bingx(bingx_config)
            exchanges.append(('BingX', bingx))
            
        except Exception as e:
            print(f"⚠️ BingX initialization failed: {e}")
        
        # KuCoin (Fallback)  
        try:
            kucoin_config = {
                'rateLimit': 600,
                'enableRateLimit': True,
                'timeout': 25000,
            }
            
            kucoin = ccxt.kucoin(kucoin_config)
            exchanges.append(('KuCoin', kucoin))
            
        except Exception as e:
            print(f"⚠️ KuCoin initialization failed: {e}")
        
        print(f"📊 Initialized {len(exchanges)} exchanges")
        return exchanges

    def fetch_15m_data(self, symbol):
        """Fetch 15-minute OHLCV data with validation"""
        candles_required = self.config['scan']['candles_required']
        
        for exchange_name, exchange in self.exchanges:
            try:
                # Fetch 15m candles
                ohlcv = exchange.fetch_ohlcv(f"{symbol}/USDT", '15m', limit=candles_required)
                
                if len(ohlcv) < 100:  # Need sufficient 15m data
                    continue
                
                # Convert to DataFrame
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                # Convert to IST properly
                df.index = df.index + pd.Timedelta(hours=5, minutes=30)
                
                # Validate data quality
                if self.validate_15m_data_quality(df, symbol):
                    return df, exchange_name
                
            except Exception as e:
                continue
        
        return None, None

    def validate_15m_data_quality(self, df, symbol):
        """Validate 15m data quality"""
        if df.empty or len(df) < 50:
            return False
        
        # Check for reasonable price ranges
        if df['close'].iloc[-1] <= 0 or df['volume'].iloc[-1] <= 0:
            return False
        
        # Check for sufficient price movement
        price_range = df['high'].max() - df['low'].min()
        if price_range / df['close'].mean() < 0.005:  # Less than 0.5% range
            return False
        
        return True

    def analyze_coin_15m(self, coin_data):
        """High-risk 15-minute analysis with proper deduplication"""
        symbol = coin_data.get('symbol', '').upper()
        
        try:
            # Fetch 15m data
            price_df, exchange_used = self.fetch_15m_data(symbol)
            if price_df is None:
                return None
            
            # Convert to Heikin-Ashi
            ha_data = heikin_ashi(price_df)
            
            # Apply CipherB indicator
            cipherb_signals = detect_cipherb_signals(ha_data, self.config['cipherb'])
            if cipherb_signals.empty:
                return None
            
            # Get latest signal data
            latest_signals = cipherb_signals.iloc[-1]
            latest_timestamp = cipherb_signals.index[-1]
            
            # Check for BUY signal
            if latest_signals['buySignal']:
                if self.deduplicator.is_allowed(symbol, 'BUY', latest_timestamp):
                    return {
                        'coin': coin_data,
                        'signal': 'BUY',
                        'wt1': latest_signals['wt1'],
                        'wt2': latest_signals['wt2'],
                        'exchange': exchange_used,
                        'timestamp': latest_timestamp
                    }
            
            # Check for SELL signal
            if latest_signals['sellSignal']:
                if self.deduplicator.is_allowed(symbol, 'SELL', latest_timestamp):
                    return {
                        'coin': coin_data,
                        'signal': 'SELL',
                        'wt1': latest_signals['wt1'],
                        'wt2': latest_signals['wt2'],
                        'exchange': exchange_used,
                        'timestamp': latest_timestamp
                    }
            
            return None
            
        except Exception as e:
            return None

    def run_analysis(self):
        """Execute high-risk 15m analysis with offset scheduling"""
        ist_current = get_ist_time()
        
        print("="*80)
        print("🔔 HIGH-RISK 15M CIPHERB ANALYSIS - OFFSET SCHEDULE")
        print("="*80)
        print(f"🕐 IST Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"⏱️ Schedule: Offset minutes (3,18,33,48)")
        print(f"🎯 System: {self.config['system']['name']}")
        
        if not self.market_data:
            print("❌ No market data available")
            return
        
        print(f"📊 Available exchanges: {[name for name, _ in self.exchanges]}")
        print(f"📈 High-risk coins to analyze: {len(self.market_data)}")
        
        # Collect all signals
        all_signals = []
        total_analyzed = 0
        batch_size = self.config['alert']['batch_size']
        
        for i in range(0, len(self.market_data), batch_size):
            batch = self.market_data[i:i+batch_size]
            batch_num = i//batch_size + 1
            print(f"\n🔄 Analyzing batch {batch_num}/{(len(self.market_data)-1)//batch_size + 1}")
            
            for coin in batch:
                signal = self.analyze_coin_15m(coin)
                if signal:
                    all_signals.append(signal)
                    print(f"🔔 {signal['signal']}: {signal['coin']['symbol'].upper()}")
                total_analyzed += 1
                time.sleep(self.config['exchange']['rate_limit_sec'])
        
        # Send batched alert if we have signals
        if all_signals:
            send_batched_alert(all_signals)
            print(f"\n✅ Sent batched alert with {len(all_signals)} signals")
        else:
            print(f"\n📊 No signals found in this run")
        
        # Analysis summary
        print(f"\n" + "="*80)
        print("🔔 HIGH-RISK 15M ANALYSIS COMPLETE")
        print("="*80)
        print(f"🎯 Total coins analyzed: {total_analyzed}")
        print(f"🚨 High-risk signals found: {len(all_signals)}")
        if total_analyzed > 0:
            print(f"📈 Signal rate: {len(all_signals)/total_analyzed*100:.1f}%")
        print(f"🕐 Completed at: {get_ist_time().strftime('%H:%M:%S IST')}")
        print("="*80)
        
        # Cleanup
        self.deduplicator.cleanup_old()

if __name__ == '__main__':
    analyzer = HighRisk15mAnalyzer()
    analyzer.run_analysis()
