"""
Precise CipherB Implementation
Detects EXACT crossover events like Pine Script plotshape
"""

import pandas as pd
import numpy as np

def ema(series, length):
    """Exponential Moving Average"""
    return series.ewm(span=length, adjust=False).mean()

def detect_precise_cipherb_signals(df, config):
    """
    Precise CipherB detection - EXACT crossover logic
    Only triggers on the bar where crossover actually happens
    """
    
    # CipherB configuration
    channel_len = config.get('wt_channel_len', 9)
    average_len = config.get('wt_average_len', 12) 
    ma_len = config.get('wt_ma_len', 3)
    oversold = config.get('oversold_threshold', -60)
    overbought = config.get('overbought_threshold', 60)
    
    # WaveTrend calculation on pure OHLCV data
    hlc3 = (df['high'] + df['low'] + df['close']) / 3
    esa = ema(hlc3, channel_len)
    d = ema(abs(hlc3 - esa), channel_len)
    ci = (hlc3 - esa) / (0.015 * d)
    wt1 = ema(ci, average_len)
    wt2 = ema(wt1, ma_len)
    
    # Create signals DataFrame
    signals = pd.DataFrame(index=df.index)
    signals['wt1'] = wt1
    signals['wt2'] = wt2
    signals['buySignal'] = False
    signals['sellSignal'] = False
    
    # PRECISE CROSSOVER DETECTION - Like Pine Script ta.crossover()
    for i in range(1, len(signals)):
        current_wt1 = wt1.iloc[i]
        previous_wt1 = wt1.iloc[i-1]
        
        # BUY: WT1 crosses ABOVE oversold threshold (from below to above)
        if previous_wt1 <= oversold and current_wt1 > oversold:
            signals.iloc[i, signals.columns.get_loc('buySignal')] = True
            print(f"🟢 BUY crossover detected: {previous_wt1:.2f} → {current_wt1:.2f} (threshold: {oversold})")
        
        # SELL: WT1 crosses BELOW overbought threshold (from above to below)
        if previous_wt1 >= overbought and current_wt1 < overbought:
            signals.iloc[i, signals.columns.get_loc('sellSignal')] = True
            print(f"🔴 SELL crossover detected: {previous_wt1:.2f} → {current_wt1:.2f} (threshold: {overbought})")
    
    return signals
