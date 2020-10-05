from ..new_utils.new_utils import *
import pandas as pd
import numpy as np


def build_actor(tables, blueprint, delays=None):
    parameters = blueprint['parameters']

    exchange = parameters.get('exchange')
    source = parameters.get('source')
    pair = parameters.get('pair')

    raw_data = tables['raw']

    data = raw_data[(raw_data['exchange'] == exchange) & (raw_data['pair'] == pair)]

    if source in ['book', 'quote']:
        data = data[['ask', 'ask_quantity', 'bid', 'bid_quantity']].rename(columns={'ask': 'ask_price', 'bid': 'bid_price'})
    else:
        data[['buy_price', 'buy_quantity']] = data[['price', 'quantity']]
        data[['sell_price', 'sell_quantity']] = data[['price', 'quantity']]

        data.loc[data['side'] == 'sell', 'buy_quantity'] = np.nan
        data.loc[data['side'] == 'buy', 'sell_quantity'] = np.nan

        data.loc[data['side'] == 'sell', 'buy_price'] = np.nan
        data.loc[data['side'] == 'buy', 'sell_price'] = np.nan

        data = data[['buy_price', 'buy_quantity', 'sell_price', 'sell_quantity']]

    data = apply_delay(data, exchange, 'account', delays=delays)
    return data
