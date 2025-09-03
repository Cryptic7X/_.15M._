"""
FIXED Timestamp Utilities - Handles UTC/IST conversion properly
"""

# At top of timestamp_utils.py
import datetime
import pytz

IST = pytz.timezone('Asia/Kolkata')
UTC = pytz.UTC

def get_current_ist_time():
    """Get current time in IST"""
    # Correctly call datetime.datetime.utcnow()
    utc_now = datetime.datetime.utcnow()
    # Localize and convert to IST
    return UTC.localize(utc_now).astimezone(IST).replace(tzinfo=None)

def get_15m_candle_boundaries(timestamp, timeframe_minutes=15):
    """
    Get exact 15-minute candle boundaries with proper timezone handling
    """
    # Ensure we have a UTC datetime
    if hasattr(timestamp, 'tzinfo'):
        if timestamp.tzinfo is None:
            # Assume it's UTC if naive
            utc_dt = UTC.localize(timestamp)
        else:
            # Convert to UTC
            utc_dt = timestamp.astimezone(UTC)
    else:
        # Plain datetime - assume UTC
        utc_dt = UTC.localize(timestamp)
    
    # Remove timezone for calculation
    utc_naive = utc_dt.replace(tzinfo=None)
    
    # Floor to 15-minute boundary
    minutes = (utc_naive.minute // timeframe_minutes) * timeframe_minutes
    candle_start_utc = utc_naive.replace(minute=minutes, second=0, microsecond=0)
    candle_close_utc = candle_start_utc + timedelta(minutes=timeframe_minutes)
    
    # Convert to IST
    candle_start_ist = (UTC.localize(candle_start_utc)).astimezone(IST).replace(tzinfo=None)
    candle_close_ist = (UTC.localize(candle_close_utc)).astimezone(IST).replace(tzinfo=None)
    
    return {
        'candle_start_utc': candle_start_utc,
        'candle_close_utc': candle_close_utc,
        'candle_start_ist': candle_start_ist,
        'candle_close_ist': candle_close_ist
    }

def should_run_analysis_now():
    """
    FIXED: Check if analysis should run now
    """
    # Get current UTC time
    now_utc = datetime.datetime.utcnow()
    
    # Find the most recent 15m candle close
    boundaries = get_15m_candle_boundaries(now_utc)
    
    # Check if at least 2 minutes have passed since candle close
    time_since_close = now_utc - boundaries['candle_close_utc']
    
    print(f"🔍 Timing Check:")
    print(f"   Current UTC: {now_utc}")
    print(f"   Candle close UTC: {boundaries['candle_close_utc']}")
    print(f"   Time since close: {time_since_close}")
    print(f"   Should run: {time_since_close >= timedelta(minutes=2) and time_since_close < timedelta(minutes=15)}")
    
    # Run if 2-15 minutes after candle close
    return (time_since_close >= timedelta(minutes=2) and 
            time_since_close < timedelta(minutes=15))

def get_current_analysis_candle():
    """
    Get the most recently closed candle to analyze
    """
    now_utc = datetime.datetime.utcnow()
    
    # Get current candle boundaries
    current_boundaries = get_15m_candle_boundaries(now_utc)
    
    # If less than 2 minutes since close, analyze current candle
    # If more than 2 minutes, we should be analyzing it
    time_since_close = now_utc - current_boundaries['candle_close_utc']
    
    if time_since_close >= timedelta(minutes=2):
        # Analyze the candle that just closed
        return current_boundaries
    else:
        # Too soon - analyze previous candle
        prev_close_utc = current_boundaries['candle_close_utc'] - timedelta(minutes=15)
        return get_15m_candle_boundaries(prev_close_utc)

def format_ist_timestamp(dt, include_date=True):
    """Format datetime as IST string"""
    if dt.tzinfo is None:
        # If naive, assume it's already IST
        ist_dt = dt
    else:
        # Convert to IST
        ist_dt = dt.astimezone(IST).replace(tzinfo=None)
    
    if include_date:
        return ist_dt.strftime('%Y-%m-%d %H:%M:%S IST')
    else:
        return ist_dt.strftime('%H:%M:%S IST')


