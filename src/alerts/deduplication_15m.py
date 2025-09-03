"""
High-Risk 15-Minute Alert Deduplication System
Prevents duplicate alerts within the same 15-minute candle
"""

import json
import os
from datetime import datetime, timedelta

class AlertDeduplicator:
    def __init__(self):
        self.cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'alert_cache_15m.json')
        self.cache = self.load_cache()

    def load_cache(self):
        try:
            with open(self.cache_file, 'r') as f:
                return json.load(f)
        except:
            return {}

    def save_cache(self):
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f)

    def get_candle_key(self, symbol, signal_type, timestamp):
        # Convert to IST and round to 15-minute boundary
        if hasattr(timestamp, 'tzinfo') and timestamp.tzinfo:
            ts = timestamp.replace(tzinfo=None)
        else:
            ts = timestamp
        
        # Round to 15-minute candle start
        minutes = (ts.minute // 15) * 15
        candle_start = ts.replace(minute=minutes, second=0, microsecond=0)
        
        key = f"{symbol}_{signal_type}_{candle_start.strftime('%Y%m%d_%H%M')}"
        return key

    def is_allowed(self, symbol, signal_type, timestamp):
        key = self.get_candle_key(symbol, signal_type, timestamp)
        
        if key in self.cache:
            return False  # Already sent
        
        # Allow and store
        self.cache[key] = datetime.utcnow().isoformat()
        self.save_cache()
        self.cleanup_old()
        return True

    def cleanup_old(self):
        cutoff = datetime.utcnow() - timedelta(days=1)
        to_remove = []
        
        for key, timestamp_str in self.cache.items():
            try:
                ts = datetime.fromisoformat(timestamp_str)
                if ts < cutoff:
                    to_remove.append(key)
            except:
                to_remove.append(key)
        
        for key in to_remove:
            del self.cache[key]
        
        if to_remove:
            self.save_cache()

