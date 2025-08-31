#!/usr/bin/env python3
"""
High-Risk 15-Minute CipherB Analysis Engine
Real-time 15m Heikin-Ashi CipherB analysis with duplicate prevention
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
from indicators.cipherb_fixed import detect_cipherb_signals
from alerts.deduplication_15m import HighRisk15mDeduplicator
from alerts.telegram_high_risk import send_high_risk_alert

class HighRisk15mAnalyzer:
    def __init__(self):
        self.config = self.load_configuration()
        self.exchanges = self.initialize_exchanges()
        self.deduplicator = HighRisk15mDeduplicator()
        
    def load_configuration(self):
        """Load high-risk system configuration"""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def load_high_risk_market_data(self):
        """Load high-risk filtered market data"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'high_risk_market_data.json')
        
        if not os.path.exists(cache_file):
            print("❌ High-risk market data not found. Run data fetcher first.")
            return None
        
        with open(cache_file, 'r') as f:
            data = json.load(f)
        
        coins = data.get('coins', [])
        metadata = data.get('metadata', {})
        
        print(f"📊 Loaded {len(coins)} high-risk coins")
        print(f"🕐 Last updated: {metadata.get('last_updated', 'Unknown')}")
        
        return coins
    
    def initialize_exchanges(self):
        """Initialize exchange connections for 15m data"""
        exchanges = []
        
        # BingX (Primary)
        try:
            bingx_config = {
                'apiKey': os.getenv('BINGX_API_KEY', ''),
                'secret': os.getenv('BINGX_SECRET_KEY', ''),
                'sandbox': False,
                'rateLimit': 200,  # Faster for 15m
                'enableRateLimit': True,
                'timeout': self.config['exchanges']['timeout'] * 1000,
            }
            
            bingx = ccxt.bingx(bingx_config)
            exchanges.append(('BingX', bingx))
            
        except Exception as e:
            print(f"⚠️ BingX initialization failed: {e}")
        
        # KuCoin (Fallback)
        try:
            kucoin_config = {
                'rateLimit': 800,
                'enableRateLimit': True,
                'timeout': self.config['exchanges']['timeout'] * 1000,
            }
            
            kucoin = ccxt.kucoin(kucoin_config)
            exchanges.append(('KuCoin', kucoin))
            
        except Exception as e:
            print(f"⚠️ KuCoin initialization failed: {e}")
        
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
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                # Convert to IST
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
        if df['Close'].iloc[-1] <= 0 or df['Volume'].iloc[-1] <= 0:
            return False
        
        # Check for sufficient price movement
        price_range = df['High'].max() - df['Low'].min()
        if price_range / df['Close'].mean() < 0.005:  # Less than 0.5% range
            return False
        
        return True
    
    def analyze_coin_15m(self, coin_data):
        """
        High-risk 15-minute analysis with pure CipherB
        One alert per 15-minute candle
        """
        symbol = coin_data.get('symbol', '').upper()
        
        try:
            # Fetch 15m data
            price_df, exchange_used = self.fetch_15m_data(symbol)
            if price_df is None:
                return False, f"No 15m data available"
            
            # Convert to Heikin-Ashi
            ha_data = heikin_ashi(price_df)
            
            # Apply CipherB indicator
            cipherb_signals = detect_cipherb_signals(ha_data, self.config['cipherb'])
            if cipherb_signals.empty:
                return False, "No CipherB signals"
            
            # Get latest signal data
            latest_signals = cipherb_signals.iloc[-1]
            latest_timestamp = cipherb_signals.index[-1]
            
            signal_sent = False
            
            # Pure CipherB BUY signal
            if latest_signals['buySignal']:
                if self.deduplicator.is_alert_allowed(symbol, 'BUY', latest_timestamp):
                    print(f"🔔🟢 HIGH-RISK BUY: {symbol} (15m) - {exchange_used}")
                    print(f"   📊 CipherB: wt1={latest_signals['wt1']:.1f}, wt2={latest_signals['wt2']:.1f}")
                    print(f"   🕐 Timestamp: {latest_timestamp}")
                    
                    send_high_risk_alert(
                        coin_data, 'BUY',
                        latest_signals['wt1'], latest_signals['wt2'],
                        exchange_used, latest_timestamp
                    )
                    signal_sent = True
            
            # Pure CipherB SELL signal
            if latest_signals['sellSignal']:
                if self.deduplicator.is_alert_allowed(symbol, 'SELL', latest_timestamp):
                    print(f"🔔🔴 HIGH-RISK SELL: {symbol} (15m) - {exchange_used}")
                    print(f"   📊 CipherB: wt1={latest_signals['wt1']:.1f}, wt2={latest_signals['wt2']:.1f}")
                    print(f"   🕐 Timestamp: {latest_timestamp}")
                    
                    send_high_risk_alert(
                        coin_data, 'SELL',
                        latest_signals['wt1'], latest_signals['wt2'],
                        exchange_used, latest_timestamp
                    )
                    signal_sent = True
            
            return signal_sent, f"Analysis complete - {exchange_used}"
            
        except Exception as e:
            return False, f"Analysis error: {str(e)[:100]}"
    
    def run_high_risk_analysis(self):
        """Execute high-risk 15m market analysis"""
        print(f"\n" + "="*80)
        print("🔔 HIGH-RISK 15M CIPHERB ANALYSIS STARTING")
        print("="*80)
        print(f"🕐 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"⏱️ Timeframe: {self.config['system']['timeframe'].upper()}")
        print(f"🎯 System: {self.config['system']['name']}")
        print(f"🔔 Alert Mode: High-Risk Channel")
        
        # Load high-risk market data
        coins = self.load_high_risk_market_data()
        if not coins:
            return
        
        print(f"📊 Available exchanges: {[name for name, _ in self.exchanges]}")
        print(f"📈 High-risk coins to analyze: {len(coins)}")
        
        # High-speed batch processing
        batch_size = self.config['alerts']['batch_size']
        total_signals = 0
        total_analyzed = 0
        
        for i in range(0, len(coins), batch_size):
            batch = coins[i:i+batch_size]
            batch_signals = 0
            
            print(f"\n🔄 High-Risk Batch {i//batch_size + 1}: Analyzing coins {i+1}-{min(i+batch_size, len(coins))}")
            
            for coin in batch:
                signal_sent, status = self.analyze_coin_15m(coin)
                if signal_sent:
                    batch_signals += 1
                total_analyzed += 1
                
                # Fast rate limiting for 15m
                time.sleep(self.config['exchanges']['rate_limit'])
            
            total_signals += batch_signals
            print(f"✅ Batch complete: {batch_signals} high-risk signals from {len(batch)} coins")
            
            # Inter-batch delay
            if i + batch_size < len(coins):
                time.sleep(1)
        
        # Analysis summary
        print(f"\n" + "="*80)
        print("🔔 HIGH-RISK 15M ANALYSIS COMPLETE")
        print("="*80)
        print(f"🎯 Total coins analyzed: {total_analyzed}")
        print(f"🚨 High-risk signals sent: {total_signals}")
        print(f"📈 Signal efficiency: {total_signals/total_analyzed*100:.2f}%")
        print(f"⏰ Next analysis: {(datetime.now() + timedelta(minutes=15)).strftime('%H:%M:%S IST')}")
        print("="*80)
        
        # Cleanup
        self.deduplicator.cleanup_expired_entries()

def main():
    analyzer = HighRisk15mAnalyzer()
    analyzer.run_high_risk_analysis()

if __name__ == '__main__':
    main()
