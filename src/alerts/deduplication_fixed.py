"""
Fixed Deduplication - Prevents alerts for same crossover event
Tracks actual crossover timestamps, not general signal cooldowns
"""

import json
import os
from datetime import datetime, timedelta

class FixedDeduplicator:
    def __init__(self):
        self.cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'crossover_alerts_15m.json')
        self.crossover_cache = self.load_cache()
    
    def load_cache(self):
        try:
            with open(self.cache_file, 'r') as f:
                cache = json.load(f)
            print(f"📁 Loaded crossover cache: {len(cache)} entries")
            return cache
        except (FileNotFoundError, json.JSONDecodeError):
            print("📁 Starting fresh crossover cache")
            return {}
    
    def save_cache(self):
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, 'w') as f:
            json.dump(self.crossover_cache, f, indent=2)
    
    def is_crossover_allowed(self, symbol, signal_type, crossover_timestamp):
        """
        Check if this exact crossover was already alerted
        Uses crossover timestamp to prevent duplicate alerts for same event
        """
        
        # Create unique key for this crossover event
        crossover_key = f"{symbol}_{signal_type}_{crossover_timestamp.strftime('%Y%m%d_%H%M')}"
        
        print(f"🔍 Crossover check: {crossover_key}")
        
        # Check if we already sent alert for this exact crossover
        if crossover_key in self.crossover_cache:
            cached_time = self.crossover_cache[crossover_key]
            print(f"   ❌ BLOCKED - Already alerted for this crossover at: {cached_time}")
            return False
        
        # Allow this crossover and cache it
        self.crossover_cache[crossover_key] = datetime.utcnow().isoformat()
        self.save_cache()
        print(f"   ✅ ALLOWED - First alert for this crossover")
        return True
    
    def cleanup_old_crossovers(self):
        """Remove crossover entries older than 48 hours"""
        cutoff_time = datetime.utcnow() - timedelta(hours=48)
        
        old_keys = []
        for key, timestamp_str in self.crossover_cache.items():
            try:
                timestamp = datetime.fromisoformat(timestamp_str)
                if timestamp < cutoff_time:
                    old_keys.append(key)
            except ValueError:
                old_keys.append(key)
        
        for key in old_keys:
            del self.crossover_cache[key]
        
        if old_keys:
            self.save_cache()
            print(f"🧹 Cleaned {len(old_keys)} old crossover entries")
