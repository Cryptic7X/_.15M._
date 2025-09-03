#!/usr/bin/env python3
"""
Professional 15-Minute CipherB Analysis System
Based on proven 2h system logic - simplified and reliable
NO Heikin Ashi - Pure CipherB signals like your working 2h system
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

from alerts.telegram_professional import send_professional_alert
from alerts.deduplication_professional import ProfessionalDeduplicator
from indicators.cipherb_pure import detect_pure_cipherb_signals

def get_ist_time():
    """Convert UTC to IST for proper timezone display"""
    utc_now = datetime.utcnow()
    ist_time = utc_now + timedelta(hours=5, minutes=30)
    return ist_time

class Professional15mAnalyzer:
    def __init__(self):
        self.config = self.load_config()
        self.deduplicator = ProfessionalDeduplicator(cooldown_hours=0.25)  # 15-minute cooldown
        self.exchanges = self.init_exchanges()
        self.market_data = self.load_market_data()
        
        print(f"🚀 Professional 15m System Initialized")
        print(f"📊 Market coins loaded: {len(self.market_data)}")
        print(f"🔄 Exchanges available: {[name for name, _ in self.exchanges]}")

    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path) as f:
            return yaml.safe_load(f)

    def load_market_data(self):
        """Load market data from cache - same as 2h system"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'high_risk_market_data.json')
        
        if not os.path.exists(cache_file):
            print("❌ Market data cache not found")
            return []
        
        with open(cache_file) as f:
            data = json.load(f)
        
        coins = data.get('coins', [])
        return coins

    def init_exchanges(self):
        """Initialize exchanges - same as 2h system"""
        exchanges = []
        
        # BingX (Primary)
        try:
            bingx = ccxt.bingx({
                'apiKey': os.getenv('BINGX_API_KEY', ''),
                'secret': os.getenv('BINGX_SECRET_KEY', ''),
                'sandbox': False,
                'rateLimit': 300,
                'enableRateLimit': True,
                'timeout': 30000,
            })
            exchanges.append(('BingX', bingx))
            
        except Exception as e:
            print(f"⚠️ BingX init failed: {e}")
        
        # KuCoin (Fallback)  
        try:
            kucoin = ccxt.kucoin({
                'rateLimit': 500,
                'enableRateLimit': True,
                'timeout': 30000,
            })
            exchanges.append(('KuCoin', kucoin))
            
        except Exception as e:
            print(f"⚠️ KuCoin init failed: {e}")
        
        return exchanges

    def fetch_ohlcv_data(self, symbol, timeframe='15m', limit=200):
        """Fetch raw OHLCV data - same as 2h system, just different timeframe"""
        
        for exchange_name, exchange in self.exchanges:
            try:
                # Fetch candles
                ohlcv = exchange.fetch_ohlcv(f"{symbol}/USDT", timeframe, limit=limit)
                
                if len(ohlcv) < 50:  # Need minimum data
                    continue
                
                # Convert to DataFrame - PURE DATA, no Heikin Ashi conversion!
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                # Convert to IST
                df.index = df.index + pd.Timedelta(hours=5, minutes=30)
                
                # Basic validation
                if self.validate_data_quality(df):
                    return df, exchange_name
                
            except Exception as e:
                continue
        
        return None, None

    def validate_data_quality(self, df):
        """Basic data validation - same as 2h system"""
        if df.empty or len(df) < 20:
            return False
        
        # Check for valid prices
        if df['close'].iloc[-1] <= 0 or df['volume'].iloc[-1] <= 0:
            return False
        
        return True

    def analyze_coin_professional(self, coin_data):
        """
        Professional analysis - EXACTLY like 2h system but 15m timeframe
        NO Heikin Ashi - Pure CipherB on raw OHLCV data
        """
        symbol = coin_data.get('symbol', '').upper()
        
        try:
            # Fetch pure OHLCV data (NO Heikin Ashi conversion!)
            price_df, exchange_used = self.fetch_ohlcv_data(symbol, '15m')
            if price_df is None:
                return None
            
            # Apply PURE CipherB indicator directly on raw data
            signals_df = detect_pure_cipherb_signals(price_df, self.config['cipherb'])
            if signals_df.empty:
                return None
            
            # Get latest signal - same logic as 2h system
            latest_signal = signals_df.iloc[-1]
            signal_timestamp = signals_df.index[-1]
            
            # Check for BUY signal with deduplication
            if latest_signal['buySignal']:
                if self.deduplicator.is_signal_allowed(symbol, 'BUY', signal_timestamp):
                    return {
                        'symbol': symbol,
                        'signal': 'BUY',
                        'wt1': latest_signal['wt1'],
                        'wt2': latest_signal['wt2'],
                        'price': coin_data.get('current_price', 0),
                        'change_24h': coin_data.get('price_change_percentage_24h', 0),
                        'market_cap': coin_data.get('market_cap', 0),
                        'exchange': exchange_used,
                        'timestamp': signal_timestamp,
                        'coin_data': coin_data
                    }
            
            # Check for SELL signal with deduplication
            if latest_signal['sellSignal']:
                if self.deduplicator.is_signal_allowed(symbol, 'SELL', signal_timestamp):
                    return {
                        'symbol': symbol,
                        'signal': 'SELL',
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
            print(f"❌ {symbol} analysis failed: {str(e)[:50]}")
            return None

    def run_professional_analysis(self):
        """
        Main analysis loop - same structure as 2h system
        """
        ist_current = get_ist_time()
        
        print("="*80)
        print("🔔 PROFESSIONAL 15M CIPHERB ANALYSIS")
        print("="*80)
        print(f"🕐 IST Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"⚙️ System: Back to 2h System Logic (15m timeframe)")
        print(f"📊 Pure CipherB (NO Heikin Ashi)")
        print(f"🎯 Coins to analyze: {len(self.market_data)}")
        
        if not self.market_data:
            print("❌ No market data available")
            return
        
        # Process coins in batches - same as 2h system
        signals_found = []
        batch_size = 20  # Smaller batches for reliability
        total_analyzed = 0
        
        for i in range(0, len(self.market_data), batch_size):
            batch = self.market_data[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(self.market_data) - 1) // batch_size + 1
            
            print(f"\n🔄 Processing batch {batch_num}/{total_batches}")
            
            for coin in batch:
                signal_result = self.analyze_coin_professional(coin)
                if signal_result:
                    signals_found.append(signal_result)
                    print(f"🚨 {signal_result['signal']}: {signal_result['symbol']}")
                
                total_analyzed += 1
                time.sleep(0.5)  # Rate limiting
        
        # Send alerts for each signal immediately (like 2h system)
        if signals_found:
            for signal in signals_found:
                send_professional_alert(signal)
                print(f"📲 Alert sent: {signal['symbol']} {signal['signal']}")
                time.sleep(1)  # Prevent spam
            
            print(f"\n✅ Sent {len(signals_found)} individual alerts")
        else:
            print(f"\n📊 No signals found in this analysis")
        
        print(f"\n" + "="*80)
        print("🔔 PROFESSIONAL ANALYSIS COMPLETE")
        print("="*80)
        print(f"🎯 Total coins analyzed: {total_analyzed}")
        print(f"🚨 Signals found: {len(signals_found)}")
        if total_analyzed > 0:
            print(f"📊 Signal rate: {len(signals_found)/total_analyzed*100:.1f}%")
        print("="*80)

if __name__ == '__main__':
    analyzer = Professional15mAnalyzer()
    analyzer.run_professional_analysis()
