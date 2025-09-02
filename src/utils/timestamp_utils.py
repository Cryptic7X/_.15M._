"""
Perfect Timestamp Utilities for 15-Minute CipherB System
Fixes all timing and timezone issues
"""

import datetime
import pandas as pd
from datetime import timedelta

def get_15m_candle_boundaries(utc_timestamp, timeframe_minutes=15):
    """
    Get exact 15-minute candle start and close times in both UTC and IST
    
    Args:
        utc_timestamp: pandas Timestamp or datetime in UTC
        timeframe_minutes: Candle timeframe (default 15)
    
    Returns:
        dict: {
            'candle_start_utc': datetime,
            'candle_close_utc': datetime, 
            'candle_start_ist': datetime,
            'candle_close_ist': datetime
        }
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
    Check if analysis should run now (2+ minutes after 15m candle close)
    """
    now_utc = datetime.datetime.utcnow()
    boundaries = get_15m_candle_boundaries(now_utc)
    
    # Check if we're at least 2 minutes past candle close
    time_since_close = now_utc - boundaries['candle_close_utc']
    return time_since_close >= timedelta(minutes=2)

def get_current_analysis_candle():
    """
    Get the candle we should be analyzing right now
    (the most recently closed 15m candle)
    """
    now_utc = datetime.datetime.utcnow()
    
    # Go back to find the most recent closed candle
    # If we're within 2 minutes of close, go back one more candle
    if now_utc.minute % 15 < 2:
        analysis_time = now_utc - timedelta(minutes=15)
    else:
        analysis_time = now_utc
    
    return get_15m_candle_boundaries(analysis_time)
