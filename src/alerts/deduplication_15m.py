"""
High-Risk 15-Minute Alert Deduplication System
Prevents duplicate alerts within the same 15-minute candle
"""

"""
Fixed 15-Minute Alert Deduplication System
Perfect candle alignment with zero timing issues
"""

import json
import os
from datetime import datetime, timedelta
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from timestamp_utils import get_15m_candle_boundaries, format_ist_timestamp

class HighRisk15mDeduplicator:
    def __init__(self):
        self.cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'alert_cache_15m.json')
        self.cache = self.load_persistent_cache()
    
    def load_persistent_cache(self):
        """Load cache from file"""
        try:
            with open(self.cache_file, 'r') as f:
                cache = json.load(f)
            print(f"📁 Loaded 15m alert cache: {len(cache)} entries")
            return cache
        except (FileNotFoundError, json.JSONDecodeError):
            print("📁 No existing 15m cache found, starting fresh")
            return {}
    
    def save_persistent_cache(self):
        """Save cache to file"""
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f, indent=2)
    
    def is_alert_allowed(self, symbol, signal_type, signal_timestamp):
        """
        Check if alert is allowed for this 15-minute candle
        Uses perfect timestamp alignment
        """
        # Get candle boundaries
        boundaries = get_15m_candle_boundaries(signal_timestamp)
        candle_close_ist = boundaries['candle_close_ist']
        
        # Create deduplication key using IST candle close time
        key = f"{symbol}_{signal_type}_{candle_close_ist.strftime('%Y%m%d_%H%M')}"
        
        print(f"🔍 Dedup check: {symbol} {signal_type}")
        print(f"   Signal timestamp: {signal_timestamp}")
        print(f"   Candle close IST: {format_ist_timestamp(candle_close_ist)}")
        print(f"   Cache key: {key}")
        
        if key in self.cache:
            cached_time = self.cache[key]
            print(f"   ❌ BLOCKED - Already sent at: {cached_time}")
            return False
        
        # Allow alert and cache it
        self.cache[key] = datetime.utcnow().isoformat()
        self.save_persistent_cache()
        print(f"   ✅ ALLOWED - First alert for this candle")
        return True
    
    def cleanup_expired_entries(self):
        """Remove entries older than 24 hours"""
        current_time = datetime.utcnow()
        expired_keys = []
        
        for key, timestamp_str in self.cache.items():
            try:
                cached_time = datetime.fromisoformat(timestamp_str)
                if current_time - cached_time > timedelta(hours=24):
                    expired_keys.append(key)
            except ValueError:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.cache[key]
        
        if expired_keys:
            self.save_persistent_cache()
            print(f"🧹 Cleaned up {len(expired_keys)} expired entries")

