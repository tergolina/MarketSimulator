from datetime import timedelta, datetime
import pandas as pd
import numpy as np


def apply_delay(data, exchange, source, delays=None):
    if delays is None:
        delays = get_delays()
    s = 'book' if source == 'quote' else source
    s = 'trade' if s == 'raw_trade' else s
    delay = delays[exchange][s]
    if delay < 0:
        data.index = data.index - timedelta(milliseconds=abs(delay))
    elif delay > 0:
        data.index = data.index + timedelta(milliseconds=abs(delay))
    return data

def get_delays():
    default = {'bitfinex':  {'book': 0,     'trade': 200,   'account': -200},
               'binance':   {'book': 100,   'trade': 200,   'account': -20},
               'bitmex':    {'book': 2500,  'trade': 2500,  'account': -2000},
               'hitbtc':    {'book': 200,   'trade': 200,   'account': -200},
               'kraken':    {'book': 200,   'trade': 200,   'account': -200},
               'poloniex':  {'book': 300,   'trade': 300,   'account': -300}}
    return default

def rolling_window(a, window):
    shape = a.shape[:-1] + (a.shape[-1] - window + 1, window)
    strides = a.strides + (a.strides[-1],)
    return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)

def group(table, group_method):
    # print (datetime.now(), '- [ Reference ] Grouping windows...')
    if isinstance(table.iloc[0,-1], np.ndarray):
        if group_method == 'std':
            return table.applymap(lambda x:x.std())
        else:
            return table.applymap(lambda x:x.mean())
    return table
