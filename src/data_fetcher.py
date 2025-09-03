#!/usr/bin/env python3
import os
import time
import json
import requests
import yaml
from datetime import datetime
import argparse

class MarketDataFetcher:
    def __init__(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.blocked_coins = self.load_blocked_coins()

    def load_blocked_coins(self):
        blocked = set()
        path = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        if os.path.exists(path):
            with open(path) as f:
                for line in f:
                    symbol = line.strip().upper()
                    if symbol and not symbol.startswith('#'):
                        blocked.add(symbol)
        return blocked

    def fetch_market_coins(self):
        base_url = self.config['apis']['coingecko']['base_url']
        coins = []
        headers = {}
        api_key = os.getenv('COINGECKO_API_KEY')
        if api_key:
            headers['X-CoinGecko-Api-Key'] = api_key

        for page in range(1, self.config['scan']['pages'] + 1):
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': self.config['scan']['coins_per_page'],
                'page': page,
                'sparkline': 'false'
            }
            try:
                response = requests.get(f"{base_url}/coins/markets", 
                                     params=params, headers=headers, 
                                     timeout=self.config['apis']['timeout'])
                response.raise_for_status()
                data = response.json()
                if not data:
                    break
                coins.extend(data)
                print(f"Fetched page {page}: {len(data)} coins")
                time.sleep(self.config['apis']['rate_limit'])
            except Exception as e:
                print(f"Error fetching page {page}: {e}")
                continue

        return coins

    def filter_coins(self, coins):
        min_cap = self.config['filters']['min_market_cap']
        min_vol = self.config['filters']['min_volume_24h']
        filtered = []
        
        for coin in coins:
            try:
                symbol = coin.get('symbol', '').upper()
                if symbol in self.blocked_coins:
                    continue
                
                cap = coin.get('market_cap', 0) or 0
                vol = coin.get('total_volume', 0) or 0
                
                if cap >= min_cap and vol >= min_vol:
                    filtered.append(coin)
            except:
                continue

        print(f"Filtered to {len(filtered)} high-quality coins")
        return filtered

    def save_to_cache(self, coins):
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        cache_file = os.path.join(cache_dir, 'high_risk_market_data.json')
        cache_data = {
            'coins': coins,
            'updated': datetime.utcnow().isoformat(),
            'count': len(coins)
        }
        
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
        
        print(f"Saved {len(coins)} coins to cache")

    def run_daily_scan(self):
        print(f"🚀 Starting market data fetch at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        coins = self.fetch_market_coins()
        filtered = self.filter_coins(coins)
        self.save_to_cache(filtered)
        print("✅ Market data fetch complete")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--daily-scan', action='store_true', help='Run daily scan')
    args = parser.parse_args()
    
    fetcher = MarketDataFetcher()
    if args.daily_scan:
        fetcher.run_daily_scan()
    else:
        print("Use --daily-scan flag")
