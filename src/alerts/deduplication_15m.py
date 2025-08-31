"""
High-Risk 15-Minute Alert Deduplication System
Prevents duplicate alerts within the same 15-minute candle
"""

import json
import os
from datetime import datetime, timedelta

class HighRisk15mDeduplicator:
    def __init__(self):
        self.cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'alert_cache_15m.json')
        self.cache = self.load_persistent_cache()
    
    def load_persistent_cache(self):
        """Load cache from file to persist between runs"""
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
    
    def get_15m_candle_start(self, signal_timestamp):
        """
        Calculate 15-minute candle start timestamp
        15m boundaries: 00:00, 00:15, 00:30, 00:45, etc.
        """
        # Convert to timezone-naive datetime if needed
        if hasattr(signal_timestamp, 'tzinfo') and signal_timestamp.tzinfo is not None:
            ts = signal_timestamp.replace(tzinfo=None)
        else:
            ts = signal_timestamp
        
        # Floor to nearest 15-minute boundary
        minutes = (ts.minute // 15) * 15
        candle_start = ts.replace(minute=minutes, second=0, microsecond=0)
        
        print(f"🕐 Signal at {ts.strftime('%H:%M:%S')} → 15m candle: {candle_start.strftime('%H:%M')}")
        return candle_start
    
    def is_alert_allowed(self, symbol, signal_type, signal_timestamp):
        """
        Check if alert is allowed for this 15-minute candle
        Only one alert per coin per signal per 15m candle
        """
        candle_start = self.get_15m_candle_start(signal_timestamp)
        key = f"{symbol}_{signal_type}_{candle_start.strftime('%Y%m%d_%H%M')}"
        
        print(f"🔍 Dedup check: {symbol} {signal_type}")
        print(f"   Candle start: {candle_start.strftime('%Y-%m-%d %H:%M')}")
        print(f"   Cache key: {key}")
        
        if key in self.cache:
            cached_time = self.cache[key]
            print(f"   ❌ BLOCKED - Already sent at: {cached_time}")
            return False
        
        # Allow alert and cache it
        self.cache[key] = datetime.utcnow().isoformat()
        self.save_persistent_cache()
        print(f"   ✅ ALLOWED - First alert for this 15m candle")
        return True
    
    def cleanup_expired_entries(self):
        """Remove entries older than 24 hours (96 fifteen-minute candles)"""
        current_time = datetime.utcnow()
        expired_keys = []
        
        for key, timestamp_str in self.cache.items():
            try:
                cached_time = datetime.fromisoformat(timestamp_str)
                if current_time - cached_time > timedelta(hours=24):
                    expired_keys.append(key)
            except ValueError:
                expired_keys.append(key)  # Remove invalid entries
        
        for key in expired_keys:
            del self.cache[key]
        
        if expired_keys:
            self.save_persistent_cache()
            print(f"🧹 Cleaned up {len(expired_keys)} expired 15m cache entries")
