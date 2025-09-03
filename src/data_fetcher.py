#!/usr/bin/env python3
import os
import time
import json
import requests
import yaml
from datetime import datetime
import argparse
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class RobustMarketDataFetcher:
    def __init__(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.blocked_coins = self.load_blocked_coins()
        self.session = self.create_robust_session()

    def create_robust_session(self):
        """Create requests session with API key authentication"""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # Set headers with API key authentication
        session.headers.update({
            'User-Agent': 'CipherB-15m-System/1.0',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        
        # Add CoinGecko Demo API key
        api_key = os.getenv('COINGECKO_API_KEY')
        if api_key:
            session.headers['x-cg-demo-api-key'] = api_key  # ← This is the key fix!
            print("✅ Using CoinGecko Demo API Key")
            print(f"   API Key: {api_key[:8]}...{api_key[-4:]}")  # Show partial key
        else:
            print("⚠️ No CoinGecko API Key found - using public limits")
            print("   Set COINGECKO_API_KEY environment variable")
        
        return session

    def load_blocked_coins(self):
        blocked = set()
        path = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        if os.path.exists(path):
            with open(path) as f:
                for line in f:
                    symbol = line.strip().upper()
                    if symbol and not symbol.startswith('#'):
                        blocked.add(symbol)
        print(f"📋 Loaded {len(blocked)} blocked coins")
        return blocked

    def handle_rate_limit(self, response):
        """Handle rate limiting with proper delays"""
        if response.status_code == 429:
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                wait_time = int(retry_after)
                print(f"⏳ Rate limited. Waiting {wait_time} seconds...")
                time.sleep(wait_time + 1)  # Add 1 second buffer
                return True
            else:
                # Default wait for rate limiting
                wait_time = 60
                print(f"⏳ Rate limited (no Retry-After header). Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                return True
        return False

    def fetch_market_coins(self):
        """Fetch coins using your Demo API key for higher limits"""
        base_url = self.config['apis']['coingecko']['base_url']
        coins = []
        
        print(f"🚀 Starting CoinGecko API fetch with authentication...")
        
        # Use your configured values (not hard-coded limits!)
        pages = self.config['scan']['pages']  # Should be 4
        per_page = min(self.config['scan']['coins_per_page'], 250)  # Max 250 with Demo API
        
        print(f"📊 Target: {pages} pages × {per_page} coins = {pages * per_page} total coins")
        
        for page in range(1, pages + 1):
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': per_page,  # Use your config value
                'page': page,
                'sparkline': 'false',
                'price_change_percentage': '24h'
            }
            
            print(f"\n📄 Fetching page {page}/{pages} (requesting {per_page} coins)")
            
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    url = f"{base_url}/coins/markets"
                    response = self.session.get(url, params=params, timeout=60)
                    
                    # Handle rate limiting
                    if response.status_code == 429:
                        retry_after = response.headers.get('Retry-After', '60')
                        wait_time = int(retry_after)
                        print(f"⏳ Rate limited. Waiting {wait_time} seconds...")
                        time.sleep(wait_time + 1)
                        continue
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    if not data:
                        print(f"📭 No data for page {page} - stopping")
                        break
                        
                    coins.extend(data)
                    print(f"✅ Page {page}: {len(data)} coins fetched")
                    break
                    
                except Exception as e:
                    print(f"❌ Attempt {attempt + 1} failed: {str(e)[:100]}")
                    if attempt == max_attempts - 1:
                        print(f"❌ All attempts failed for page {page}")
                        break
                    time.sleep((2 ** attempt) + 1)
            
            # Rate limiting between pages
            if page < pages:
                rate_limit_delay = self.config['apis']['coingecko']['rate_limit']
                print(f"⏳ Waiting {rate_limit_delay}s before next page...")
                time.sleep(rate_limit_delay)
    
        print(f"\n📊 Total coins fetched: {len(coins)}")
        return coins

    def filter_coins(self, coins):
        """Filter coins with detailed stats"""
        min_cap = self.config['filters']['min_market_cap']
        min_vol = self.config['filters']['min_volume_24h']
        filtered = []
        
        stats = {
            'total': len(coins),
            'blocked': 0,
            'low_cap': 0,
            'low_volume': 0,
            'invalid_data': 0,
            'qualified': 0
        }
        
        for coin in coins:
            try:
                symbol = coin.get('symbol', '').upper()
                
                # Check blocked coins
                if symbol in self.blocked_coins:
                    stats['blocked'] += 1
                    continue
                
                # Validate data
                cap = coin.get('market_cap')
                vol = coin.get('total_volume')
                price = coin.get('current_price')
                
                if not all([cap, vol, price]) or any(x <= 0 for x in [cap, vol, price]):
                    stats['invalid_data'] += 1
                    continue
                
                # Apply filters
                if cap < min_cap:
                    stats['low_cap'] += 1
                    continue
                    
                if vol < min_vol:
                    stats['low_volume'] += 1
                    continue
                
                filtered.append(coin)
                stats['qualified'] += 1
                
            except Exception:
                stats['invalid_data'] += 1
                continue

        # Print detailed stats
        print(f"\n📋 FILTERING RESULTS:")
        print(f"   📊 Total processed: {stats['total']}")
        print(f"   ✅ Qualified: {stats['qualified']}")
        print(f"   🚫 Blocked: {stats['blocked']}")
        print(f"   📊 Below ${min_cap/1_000_000:.0f}M cap: {stats['low_cap']}")
        print(f"   📈 Below ${min_vol/1_000_000:.0f}M volume: {stats['low_volume']}")
        print(f"   ❌ Invalid data: {stats['invalid_data']}")
        print(f"   🎯 Success rate: {stats['qualified']/stats['total']*100:.1f}%")
        
        return filtered

    def save_to_cache(self, coins):
        """Save with comprehensive metadata"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        cache_file = os.path.join(cache_dir, 'high_risk_market_data.json')
        cache_data = {
            'coins': coins,
            'metadata': {
                'last_updated': datetime.utcnow().isoformat(),
                'count': len(coins),
                'system': self.config['system']['name'],
                'version': self.config['system']['version'],
                'filters': {
                    'min_market_cap': self.config['filters']['min_market_cap'],
                    'min_volume_24h': self.config['filters']['min_volume_24h']
                }
            }
        }
        
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
        
        print(f"💾 Saved {len(coins)} qualified coins to cache")
        return cache_file

    def run_daily_scan(self):
        """Execute complete fetch process with error handling"""
        print("="*80)
        print("🚀 ROBUST MARKET DATA FETCH STARTING")
        print("="*80)
        print(f"🕐 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Fetch coins
            coins = self.fetch_market_coins()
            
            if not coins:
                print("❌ No coins fetched - saving empty cache")
                self.save_to_cache([])
                return
            
            # Filter coins
            filtered = self.filter_coins(coins)
            
            # Save to cache (even if empty to prevent analyzer errors)
            cache_file = self.save_to_cache(filtered)
            
            print("\n" + "="*80)
            print("✅ MARKET DATA FETCH COMPLETE")
            print("="*80)
            print(f"📊 Total fetched: {len(coins)}")
            print(f"✅ Qualified coins: {len(filtered)}")
            print(f"💾 Cache location: {cache_file}")
            if filtered:
                print(f"📈 Top coins: {', '.join([c['symbol'].upper() for c in filtered[:5]])}")
            print("="*80)
            
        except Exception as e:
            print(f"\n❌ Critical error: {e}")
            import traceback
            print(f"📋 Full error: {traceback.format_exc()}")
            
            # Save empty cache to prevent downstream errors
            self.save_to_cache([])
            print("💾 Saved empty cache to prevent analyzer errors")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Robust Market Data Fetcher')
    parser.add_argument('--daily-scan', action='store_true', help='Run daily market scan')
    args = parser.parse_args()
    
    fetcher = RobustMarketDataFetcher()
    if args.daily_scan:
        fetcher.run_daily_scan()
    else:
        print("Use --daily-scan flag to run the fetcher")

