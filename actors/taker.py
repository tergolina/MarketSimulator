from .actor import build_actor
import pandas as pd
import numpy as np


def build_taker(tables, blueprint, delays=None):
    parameters = blueprint.get('parameters')

    guide_name = parameters.get('guide')
    reference_name = parameters.get('reference')
    hedge_name = parameters.get('hedge')

    guide = tables[guide_name] if guide_name else None
    hedge = tables[hedge_name] if hedge_name else None
    reference = tables[reference_name]

    table_list = [build_actor(tables, blueprint)]
    table_list += [reference[['bid', 'ask']].rename(columns={'bid': 'bid_reference', 'ask': 'ask_reference'})]
    if hedge is not None:
        table_list += [hedge]

    table = pd.concat(table_list, sort=False).sort_index().dropna(how='all')
    if 'hedge_bid_price' not in table.columns:
        table['hedge_bid_price'] = np.nan
        table['hedge_ask_price'] = np.nan

    table[['bid_reference', 'ask_reference', 'hedge_bid_price', 'hedge_ask_price']] = table[['bid_reference', 'ask_reference', 'hedge_bid_price', 'hedge_ask_price']].ffill()
    table = table[((table['bid_price'] == table['bid_price']) | (table['ask_price'] == table['ask_price'])) & (table['bid_reference'] == table['bid_reference'])]

    if hedge is not None:
        table = table[table['hedge_bid_price'] == table['hedge_bid_price']]

    return edit_taker(table, parameters)

def edit_taker(table, parameters, delays=None):
    premia = parameters.get('premia')
    open_premia = parameters.get('open_premia')
    close_premia = parameters.get('close_premia')

    open_premia = open_premia if open_premia else premia
    close_premia = close_premia if close_premia else open_premia

    table['open_buy_price'] = table['bid_reference'] * (1 - open_premia)
    table['close_buy_price'] = table['bid_reference'] * (1 - close_premia)

    table['open_sell_price'] = table['ask_reference'] * (1 + open_premia)
    table['close_sell_price'] = table['ask_reference'] * (1 + close_premia)

    return table
