"""
Perfect Timestamp Utilities for 15-Minute CipherB System
Fixed to always analyze the correct closed candle
"""

import datetime
import pandas as pd
from datetime import timedelta

def get_15m_candle_boundaries(utc_timestamp, timeframe_minutes=15):
    """
    Get exact 15-minute candle start and close times in both UTC and IST
    """
    # Convert to naive UTC datetime
    if hasattr(utc_timestamp, 'tzinfo') and utc_timestamp.tzinfo is not None:
        utc_dt = utc_timestamp.tz_convert('UTC').tz_localize(None)
    elif isinstance(utc_timestamp, pd.Timestamp):
        utc_dt = utc_timestamp.to_pydatetime()
    else:
        utc_dt = utc_timestamp
    
    # Floor to 15-minute boundary in UTC
    minutes = (utc_dt.minute // timeframe_minutes) * timeframe_minutes
    candle_start_utc = utc_dt.replace(minute=minutes, second=0, microsecond=0)
    candle_close_utc = candle_start_utc + timedelta(minutes=timeframe_minutes)
    
    # Convert to IST (UTC + 5:30)
    ist_offset = timedelta(hours=5, minutes=30)
    candle_start_ist = candle_start_utc + ist_offset
    candle_close_ist = candle_close_utc + ist_offset
    
    return {
        'candle_start_utc': candle_start_utc,
        'candle_close_utc': candle_close_utc,
        'candle_start_ist': candle_start_ist,
        'candle_close_ist': candle_close_ist
    }

def format_ist_timestamp(dt, include_date=True):
    """Format datetime as IST string"""
    if include_date:
        return dt.strftime('%Y-%m-%d %H:%M:%S IST')
    else:
        return dt.strftime('%H:%M:%S IST')

def should_run_analysis_now():
    """
    Always return True since GitHub Actions cron handles timing
    """
    return True

def get_current_analysis_candle():
    """
    Get the most recently CLOSED 15m candle that we should analyze
    """
    now_utc = datetime.datetime.utcnow()
    
    # Go back 1 candle to ensure we analyze a fully closed candle
    # If current time is 05:08 UTC, we want to analyze 04:45-05:00 candle
    analysis_time = now_utc - timedelta(minutes=15)
    
    boundaries = get_15m_candle_boundaries(analysis_time)
    
    print(f"🔍 Debug timing:")
    print(f"   Current UTC: {now_utc.strftime('%H:%M:%S')}")
    print(f"   Analysis time: {analysis_time.strftime('%H:%M:%S')}")
    print(f"   Candle UTC: {boundaries['candle_start_utc'].strftime('%H:%M')}-{boundaries['candle_close_utc'].strftime('%H:%M')}")
    print(f"   Candle IST: {boundaries['candle_start_ist'].strftime('%H:%M')}-{boundaries['candle_close_ist'].strftime('%H:%M')}")
    
    return boundaries
