#!/usr/bin/env python3
"""
High-Risk 15-Minute Market Data Fetcher
Optimized for rapid 15m candle analysis with high-volatility coins
"""

import requests
import json
import os
import time
from datetime import datetime, timedelta
import yaml

class HighRisk15mDataFetcher:
    def __init__(self):
        self.config = self.load_config()
        self.blocked_coins = self.load_blocked_coins()
        
    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def load_blocked_coins(self):
        """Load blocked coins from both config and file"""
        blocked_coins = set()
        
        # Load from config (YAML)
        config_blocked = self.config.get('blocked_coins', [])
        blocked_coins.update(coin.upper() for coin in config_blocked)
        
        # Load from blocked_coins.txt file
        blocked_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        try:
            with open(blocked_file, 'r') as f:
                for line in f:
                    line = line.strip().upper()
                    # Skip empty lines and comments
                    if line and not line.startswith('#'):
                        blocked_coins.add(line)
            print(f"📋 Loaded {len(blocked_coins)} blocked coins from config and file")
        except FileNotFoundError:
            print("⚠️ No blocked_coins.txt found, using config only")
        
        return blocked_coins
    
    def fetch_coingecko_coins(self):
        """Fetch high-risk coins from CoinGecko with 15m focus and API key support"""
        url = f"{self.config['coingecko']['base_url']}/coins/markets"
        all_coins = []
        
        # Prepare headers with API key if provided
        headers = {}
        api_key = os.getenv("COINGECKO_API_KEY")
        if api_key:
            headers['X-CoinGecko-Api-Key'] = api_key
        
        print("🔍 Scanning for high-risk 15m opportunities...")
        
        for page in range(1, self.config['scan']['pages'] + 1):
            try:
                params = {
                    'vs_currency': 'usd',
                    'order': 'market_cap_desc',
                    'per_page': self.config['scan']['coins_per_page'],
                    'page': page,
                    'sparkline': 'false'
                }
                
                response = requests.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.config['coingecko']['timeout']
                )
                response.raise_for_status()
                coins = response.json()
                
                if not coins:
                    break
                    
                all_coins.extend(coins)
                print(f"📄 Page {page}: {len(coins)} coins")
                time.sleep(self.config['coingecko']['rate_limit'])
                
            except Exception as e:
                print(f"❌ Error fetching page {page}: {e}")
                continue
        
        print(f"📊 Total coins fetched: {len(all_coins)}")
        return all_coins
    
    def apply_high_risk_filters(self, coins):
        """Apply high-risk 15m trading filters"""
        print(f"\n🔍 Applying high-risk filters...")
        print(f"📊 Criteria: Market Cap ≥ ${self.config['market_filter']['min_market_cap']:,}, Volume ≥ ${self.config['market_filter']['min_volume_24h']:,}")
        
        qualified = []
        blocked_count = 0
        below_cap = 0
        below_volume = 0
        invalid_data = 0
        
        for coin in coins:
            try:
                symbol = coin.get('symbol', '').upper()
                
                # Check blocked coins
                if symbol in self.blocked_coins:
                    blocked_count += 1
                    continue
                
                # Check market cap
                market_cap = coin.get('market_cap', 0) or 0
                if market_cap < self.config['market_filter']['min_market_cap']:
                    below_cap += 1
                    continue
                
                # Check volume
                volume = coin.get('total_volume', 0) or 0
                if volume < self.config['market_filter']['min_volume_24h']:
                    below_volume += 1
                    continue
                
                # Check data validity
                if not coin.get('current_price') or market_cap <= 0 or volume <= 0:
                    invalid_data += 1
                    continue
                
                qualified.append(coin)
                
            except Exception as e:
                invalid_data += 1
                continue
        
        print(f"\n📋 HIGH-RISK FILTER RESULTS:")
        print(f"   ✅ Qualified coins: {len(qualified)}")
        print(f"   🚫 Blocked coins: {blocked_count}")
        print(f"   📊 Below market cap: {below_cap}")
        print(f"   📈 Below volume: {below_volume}")
        print(f"   ❌ Invalid data: {invalid_data}")
        print(f"   📊 Total processed: {len(coins)}")
        print(f"   🎯 Success rate: {len(qualified)/len(coins)*100:.1f}%")
        
        return qualified
    
    def save_market_data(self, qualified_coins):
        """Save qualified high-risk coins to cache"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        cache_file = os.path.join(cache_dir, 'high_risk_market_data.json')
        
        market_data = {
            'coins': qualified_coins,
            'metadata': {
                'last_updated': datetime.now().isoformat(),
                'system_version': self.config['system']['version'],
                'timeframe': self.config['system']['timeframe'],
                'total_coins': len(qualified_coins),
                'filters_applied': self.config['market_filter']
            }
        }
        
        with open(cache_file, 'w') as f:
            json.dump(market_data, f, indent=2)
        
        print(f"💾 High-risk market data saved: {len(qualified_coins)} coins")
        return cache_file
    
    def run_daily_scan(self):
        """Execute high-risk 15m market scan"""
        print(f"\n" + "="*80)
        print("🚀 HIGH-RISK 15M MARKET SCAN STARTING")
        print("="*80)
        print(f"🕐 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"⏱️ Timeframe: {self.config['system']['timeframe'].upper()}")
        print(f"🎯 System: {self.config['system']['name']}")
        
        # Fetch coins from CoinGecko
        all_coins = self.fetch_coingecko_coins()
        
        if not all_coins:
            print("❌ No coins fetched. Exiting.")
            return
        
        # Apply high-risk filters
        qualified_coins = self.apply_high_risk_filters(all_coins)
        
        # Save to cache
        cache_file = self.save_market_data(qualified_coins)
        
        print(f"\n" + "="*80)
        print("🎯 HIGH-RISK MARKET SCAN COMPLETE")
        print("="*80)
        print(f"📊 Raw coins scanned: {len(all_coins)}")
        print(f"✅ High-risk qualifying coins: {len(qualified_coins)}")
        print(f"📈 Filter efficiency: {len(qualified_coins)/len(all_coins)*100:.1f}%")
        print(f"💾 Cache file: {cache_file}")
        print("="*80)

def main():
    import argparse
    parser = argparse.ArgumentParser(description='High-Risk 15m Market Data Fetcher')
    parser.add_argument('--daily-scan', action='store_true', help='Run daily market scan')
    args = parser.parse_args()
    
    fetcher = HighRisk15mDataFetcher()
    
    if args.daily_scan:
        fetcher.run_daily_scan()
    else:
        print("Use --daily-scan to run market analysis")

if __name__ == '__main__':
    main()
