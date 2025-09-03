"""
Pure CipherB Implementation - NO Heikin Ashi
Exactly like your working 2h system indicator logic
"""

import pandas as pd
import numpy as np

def ema(series, length):
    """Exponential Moving Average"""
    return series.ewm(span=length, adjust=False).mean()

def detect_pure_cipherb_signals(df, config):
    """
    Pure CipherB detection on raw OHLCV data
    NO Heikin Ashi conversion - direct on price data like 2h system
    """
    
    # CipherB configuration
    channel_len = config.get('wt_channel_len', 9)
    average_len = config.get('wt_average_len', 12) 
    ma_len = config.get('wt_ma_len', 3)
    oversold = config.get('oversold_threshold', -60)
    overbought = config.get('overbought_threshold', 60)
    
    # WaveTrend calculation on RAW price data (not Heikin Ashi)
    hlc3 = (df['high'] + df['low'] + df['close']) / 3
    
    # ESA (Exponential Simple Average)
    esa = ema(hlc3, channel_len)
    
    # D (Deviation)
    d = ema(abs(hlc3 - esa), channel_len)
    
    # CI (Commodity Channel Index like)
    ci = (hlc3 - esa) / (0.015 * d)
    
    # WaveTrend lines
    wt1 = ema(ci, average_len)
    wt2 = ema(wt1, ma_len)
    
    # Create signals DataFrame
    signals = pd.DataFrame(index=df.index)
    signals['wt1'] = wt1
    signals['wt2'] = wt2
    signals['buySignal'] = False
    signals['sellSignal'] = False
    
    # Signal generation logic - SAME as your working 2h system
    # BUY: WT1 crosses above oversold level
    buy_condition = (wt1 > oversold) & (wt1.shift(1) <= oversold)
    signals.loc[buy_condition, 'buySignal'] = True
    
    # SELL: WT1 crosses below overbought level  
    sell_condition = (wt1 < overbought) & (wt1.shift(1) >= overbought)
    signals.loc[sell_condition, 'sellSignal'] = True
    
    return signals
