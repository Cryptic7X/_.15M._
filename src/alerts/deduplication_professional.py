"""
Professional Deduplication System
Same logic as working 2h system, adapted for 15m intervals
"""

import json
import os
from datetime import datetime, timedelta

class ProfessionalDeduplicator:
    def __init__(self, cooldown_hours=0.25):  # 15 minutes = 0.25 hours
        self.cooldown_period = timedelta(hours=cooldown_hours)
        self.cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'professional_alerts_15m.json')
        self.alert_cache = self.load_cache()
    
    def load_cache(self):
        """Load persistent alert cache"""
        try:
            with open(self.cache_file, 'r') as f:
                cache = json.load(f)
            print(f"📁 Loaded alert cache: {len(cache)} entries")
            return cache
        except (FileNotFoundError, json.JSONDecodeError):
            print("📁 Starting fresh alert cache")
            return {}
    
    def save_cache(self):
        """Save alert cache to file"""
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, 'w') as f:
            json.dump(self.alert_cache, f, indent=2)
    
    def is_signal_allowed(self, symbol, signal_type, timestamp):
        """
        Check if signal is allowed (same logic as 2h system)
        Prevents duplicate alerts within cooldown period
        """
        cache_key = f"{symbol}_{signal_type}"
        current_time = datetime.utcnow()
        
        print(f"🔍 Dedup check: {cache_key}")
        
        # Check if we have a recent alert for this symbol+signal
        if cache_key in self.alert_cache:
            last_alert_time = datetime.fromisoformat(self.alert_cache[cache_key])
            time_since_last = current_time - last_alert_time
            
            if time_since_last < self.cooldown_period:
                remaining = self.cooldown_period - time_since_last
                print(f"   ❌ BLOCKED - Last alert: {time_since_last.total_seconds():.0f}s ago")
                print(f"   ⏱️ Cooldown remaining: {remaining.total_seconds():.0f}s")
                return False
        
        # Allow signal and update cache
        self.alert_cache[cache_key] = current_time.isoformat()
        self.save_cache()
        print(f"   ✅ ALLOWED - Signal permitted")
        return True
    
    def cleanup_old_entries(self):
        """Remove entries older than 24 hours"""
        current_time = datetime.utcnow()
        cutoff_time = current_time - timedelta(hours=24)
        
        old_keys = []
        for key, timestamp_str in self.alert_cache.items():
            try:
                timestamp = datetime.fromisoformat(timestamp_str)
                if timestamp < cutoff_time:
                    old_keys.append(key)
            except ValueError:
                old_keys.append(key)  # Remove invalid entries
        
        for key in old_keys:
            del self.alert_cache[key]
        
        if old_keys:
            self.save_cache()
            print(f"🧹 Cleaned {len(old_keys)} old cache entries")
