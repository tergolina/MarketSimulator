from ..new_utils.new_utils import *
import pandas as pd
import numpy as np
import gc


def build_feeder(tables, blueprint, delays=None):
    parameters = blueprint['parameters']
    pair = parameters.get('pair')
    exchange = parameters.get('exchange')
    source = parameters.get('source')
    data = apply_delay(build_data(tables, blueprint), exchange, source, delays=delays)
    return data

def build_data(tables, blueprint):
    parameters = blueprint['parameters']
    pair = parameters.get('pair')
    exchange = parameters.get('exchange')
    source = parameters.get('source')

    raw_data = tables['raw']
    data = raw_data.copy()
    data[(data['pair'] != pair) | (data['exchange'] != exchange)] = np.nan

    if source == 'trade':
        data = data[['price', 'side']].groupby(data.index).agg({'price': 'last', 'side': 'last'}).ffill()

        # FAT = 100000000000
        # TICK_MIN = abs((data['price']*FAT - data['price'].shift(1)*FAT).dropna())
        # TICK_MIN = ((TICK_MIN[TICK_MIN > 0])/FAT).value_counts().index[0]
        data['bid'] = np.nan
        data['ask'] = np.nan
        data['bid'][(data['side'] == 'sell')] = data[(data['side'] == 'sell')]['price']
        data['ask'][(data['side'] == 'buy')] = data[(data['side'] == 'buy')]['price']
        # data['bid'][data['bid'].isnull()] = data['ask'][data['bid'].isnull()] - TICK_MIN
        # data['ask'][data['ask'].isnull()] = data['bid'][data['ask'].isnull()] + TICK_MIN

        data.rename(columns={'price': 'last'}, inplace=True)

        del data['side']
        gc.collect()
    
        data = data.ffill().dropna()
        data['bid'] = data[['bid', 'ask']].min(axis=1)
        data['ask'] = data[['bid', 'ask']].max(axis=1)

    elif source == 'raw_trade':
        data = data[['price', 'side']].groupby(data.index).agg({'price': 'last', 'side': 'last'})
    
    else:
        data = data[['bid', 'ask']].ffill()
        data = data.ffill().dropna()
        data['bid'] = data[['bid', 'ask']].min(axis=1)
        data['ask'] = data[['bid', 'ask']].max(axis=1)
    
    return data
