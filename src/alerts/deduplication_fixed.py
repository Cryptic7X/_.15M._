"""
Fixed Deduplication - Prevents alerts for same signal event
"""

import json
import os
from datetime import datetime, timedelta

class FixedDeduplicator:
    def __init__(self):
        self.cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'exact_alerts_15m.json')
        self.alert_cache = self.load_cache()
    
    def load_cache(self):
        try:
            with open(self.cache_file, 'r') as f:
                cache = json.load(f)
            print(f"📁 Loaded alert cache: {len(cache)} entries")
            return cache
        except (FileNotFoundError, json.JSONDecodeError):
            print("📁 Starting fresh alert cache")
            return {}
    
    def save_cache(self):
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, 'w') as f:
            json.dump(self.alert_cache, f, indent=2)
    
    def is_crossover_allowed(self, symbol, signal_type, signal_timestamp):
        """
        Check if this exact signal was already alerted
        """
        signal_key = f"{symbol}_{signal_type}_{signal_timestamp.strftime('%Y%m%d_%H%M')}"
        
        print(f"🔍 Signal check: {signal_key}")
        
        # Check if we already sent alert for this exact signal
        if signal_key in self.alert_cache:
            cached_time = self.alert_cache[signal_key]
            print(f"   ❌ BLOCKED - Already alerted: {cached_time}")
            return False
        
        # Allow this signal and cache it
        self.alert_cache[signal_key] = datetime.utcnow().isoformat()
        self.save_cache()
        print(f"   ✅ ALLOWED - New signal")
        return True
    
    def cleanup_old_entries(self):
        """Remove entries older than 48 hours"""
        cutoff_time = datetime.utcnow() - timedelta(hours=48)
        
        old_keys = []
        for key, timestamp_str in self.alert_cache.items():
            try:
                timestamp = datetime.fromisoformat(timestamp_str)
                if timestamp < cutoff_time:
                    old_keys.append(key)
            except ValueError:
                old_keys.append(key)
        
        for key in old_keys:
            del self.alert_cache[key]
        
        if old_keys:
            self.save_cache()
            print(f"🧹 Cleaned {len(old_keys)} old entries")

