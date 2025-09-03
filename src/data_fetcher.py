#!/usr/bin/env python3
import os
import time
import json
import requests
import yaml
from datetime import datetime
import argparse
import random
from urllib.parse import urljoin

class RobustMarketDataFetcher:
    def __init__(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.blocked_coins = self.load_blocked_coins()
        self.session = self.create_session()

    def create_session(self):
        """Create a requests session with proper configuration"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        
        # Add API key if available
        api_key = os.getenv('COINGECKO_API_KEY')
        if api_key:
            session.headers['X-CoinGecko-Api-Key'] = api_key
            print("✅ Using CoinGecko API Key")
        else:
            print("⚠️ No API Key found - using public API (lower limits)")
            
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

    def make_request_with_retry(self, url, params, max_retries=5):
        """Make API request with exponential backoff retry logic"""
        for attempt in range(max_retries):
            try:
                # Increase timeout significantly
                timeout = 60  # 60 seconds timeout
                
                print(f"🔄 Attempt {attempt + 1}/{max_retries} - Timeout: {timeout}s")
                
                response = self.session.get(
                    url, 
                    params=params, 
                    timeout=timeout,
                    verify=True  # Ensure SSL verification
                )
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    print(f"⏳ Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.Timeout:
                wait_time = min(300, (2 ** attempt) + random.uniform(0, 1))  # Max 5 minutes
                print(f"⏰ Timeout on attempt {attempt + 1}. Waiting {wait_time:.1f}s before retry...")
                time.sleep(wait_time)
                
            except requests.exceptions.ConnectionError as e:
                wait_time = min(180, (2 ** attempt) + random.uniform(0, 1))  # Max 3 minutes
                print(f"🌐 Connection error on attempt {attempt + 1}: {str(e)[:100]}")
                print(f"   Waiting {wait_time:.1f}s before retry...")
                time.sleep(wait_time)
                
            except requests.exceptions.HTTPError as e:
                if response.status_code == 503:  # Service unavailable
                    wait_time = min(300, (2 ** attempt) + random.uniform(0, 1))
                    print(f"🚫 Service unavailable. Waiting {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    print(f"❌ HTTP Error {response.status_code}: {e}")
                    break
                    
            except Exception as e:
                print(f"❌ Unexpected error on attempt {attempt + 1}: {str(e)[:100]}")
                if attempt == max_retries - 1:
                    break
                time.sleep(30)  # Short wait for unexpected errors
        
        print(f"❌ All {max_retries} attempts failed")
        return None

    def fetch_market_coins(self):
        """Fetch coins with robust error handling"""
        # Use public API endpoint
        base_url = self.config['apis']['coingecko']['base_url']
        coins = []
        
        print(f"🚀 Starting CoinGecko API fetch...")
        print(f"📡 Base URL: {base_url}")
        
        # Check API status first
        try:
            ping_response = self.session.get(f"{base_url}/ping", timeout=30)
            if ping_response.status_code == 200:
                print("✅ CoinGecko API is responding")
            else:
                print(f"⚠️ API ping returned status: {ping_response.status_code}")
        except:
            print("⚠️ Could not ping API - continuing anyway")

        # Reduce pages and per_page for reliability
        pages = min(self.config['scan']['pages'], 2)  # Max 2 pages to avoid timeouts
        per_page = min(self.config['scan']['coins_per_page'], 100)  # Max 100 per page
        
        for page in range(1, pages + 1):
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': per_page,
                'page': page,
                'sparkline': 'false',
                'price_change_percentage': '24h'
            }
            
            print(f"\n📄 Fetching page {page}/{pages} (limit: {per_page})")
            
            url = f"{base_url}/coins/markets"
            data = self.make_request_with_retry(url, params)
            
            if data is None:
                print(f"❌ Failed to fetch page {page}")
                continue
                
            if not data:  # Empty response
                print(f"📭 No data returned for page {page} - stopping")
                break
                
            coins.extend(data)
            print(f"✅ Page {page}: {len(data)} coins fetched")
            
            # Longer delay between requests to avoid rate limiting
            if page < pages:  # Don't wait after last page
                delay = self.config['apis']['rate_limit'] * 2  # Double the configured delay
                print(f"⏳ Waiting {delay}s before next request...")
                time.sleep(delay)

        print(f"\n📊 Total coins fetched: {len(coins)}")
        return coins

    def filter_coins(self, coins):
        """Filter coins with improved validation"""
        min_cap = self.config['filters']['min_market_cap']
        min_vol = self.config['filters']['min_volume_24h']
        filtered = []
        
        stats = {
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
                
                # Validate required fields
                cap = coin.get('market_cap')
                vol = coin.get('total_volume') 
                price = coin.get('current_price')
                
                if not all([cap, vol, price]) or cap <= 0 or vol <= 0 or price <= 0:
                    stats['invalid_data'] += 1
                    continue
                
                # Check market cap threshold
                if cap < min_cap:
                    stats['low_cap'] += 1
                    continue
                
                # Check volume threshold
                if vol < min_vol:
                    stats['low_volume'] += 1
                    continue
                
                filtered.append(coin)
                stats['qualified'] += 1
                
            except Exception as e:
                stats['invalid_data'] += 1
                continue

        # Print detailed filtering stats
        print(f"\n📋 FILTERING RESULTS:")
        print(f"   ✅ Qualified: {stats['qualified']}")
        print(f"   🚫 Blocked: {stats['blocked']}")
        print(f"   📊 Low market cap: {stats['low_cap']}")
        print(f"   📈 Low volume: {stats['low_volume']}")
        print(f"   ❌ Invalid data: {stats['invalid_data']}")
        print(f"   📊 Filter efficiency: {stats['qualified']/len(coins)*100:.1f}%")
        
        return filtered

    def save_to_cache(self, coins):
        """Save coins to cache with metadata"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        cache_file = os.path.join(cache_dir, 'high_risk_market_data.json')
        cache_data = {
            'coins': coins,
            'metadata': {
                'last_updated': datetime.utcnow().isoformat(),
                'count': len(coins),
                'system': 'CipherB Professional 15m',
                'filters_applied': {
                    'min_market_cap': self.config['filters']['min_market_cap'],
                    'min_volume_24h': self.config['filters']['min_volume_24h']
                }
            }
        }
        
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
        
        print(f"💾 Saved {len(coins)} coins to cache")
        return cache_file

    def run_daily_scan(self):
        """Run the complete market data fetch process"""
        print("="*80)
        print("🚀 ROBUST MARKET DATA FETCH STARTING")
        print("="*80)
        print(f"🕐 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Fetch coins with retry logic
            coins = self.fetch_market_coins()
            
            if not coins:
                print("❌ No coins fetched - creating empty cache")
                self.save_to_cache([])  # Save empty cache to prevent analyzer errors
                return
            
            # Filter qualified coins
            filtered = self.filter_coins(coins)
            
            if not filtered:
                print("⚠️ No coins passed filtering - check filter criteria")
                
            # Save to cache
            cache_file = self.save_to_cache(filtered)
            
            print("\n" + "="*80)
            print("✅ MARKET DATA FETCH COMPLETE")
            print("="*80)
            print(f"📊 Raw coins: {len(coins)}")
            print(f"✅ Qualified coins: {len(filtered)}")
            print(f"💾 Cache file: {cache_file}")
            print("="*80)
            
        except Exception as e:
            print(f"\n❌ Critical error during fetch: {e}")
            # Save empty cache to prevent analyzer errors
            self.save_to_cache([])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Robust Market Data Fetcher')
    parser.add_argument('--daily-scan', action='store_true', help='Run daily market scan')
    args = parser.parse_args()
    
    fetcher = RobustMarketDataFetcher()
    if args.daily_scan:
        fetcher.run_daily_scan()
    else:
        print("Use --daily-scan flag to run the fetcher")
