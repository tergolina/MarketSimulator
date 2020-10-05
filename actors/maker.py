from .actor import build_actor
import pandas as pd
import numpy as np


def build_maker(tables, blueprint, delays=None):
    parameters = blueprint.get('parameters')

    guide_name = parameters.get('guide')
    reference_name = parameters.get('reference')
    hedge_name = parameters.get('hedge')
    premia_name = parameters.get('premia')

    guide = tables[guide_name] if guide_name else None
    hedge = tables[hedge_name] if hedge_name else None
    premia = tables[premia_name] if isinstance(premia_name, str) and premia_name in tables else None
    reference = tables[reference_name]

    if 'last' not in reference.columns:
        reference['last'] = np.nan
    else:
        reference['bid'] = reference['last']
        reference['ask'] = reference['last']

    table_list = [build_actor(tables, blueprint, delays=delays)]
    table_list += [reference[['bid', 'ask', 'last']].rename(columns={'bid': 'bid_reference', 'ask': 'ask_reference', 'last': 'last_reference'})]
    if hedge is not None:
        table_list += [hedge]
    if guide is not None:
        table_list += [guide.rename(columns={'bid': 'bid_guide', 'ask': 'ask_guide'})]
    if premia is not None:
        table_list += [premia.rename(columns={'bid': 'bid_premia', 'ask': 'ask_premia'})]

    table = pd.concat(table_list, sort=False).sort_index(kind='mergesort').dropna(how='all')
    if 'hedge_bid_price' not in table.columns:
        table['hedge_bid_price'] = np.nan
        table['hedge_ask_price'] = np.nan

    if 'bid_guide' not in table.columns:
        table['bid_guide'] = np.nan
        table['ask_guide'] = np.nan

    if 'bid_premia' not in table.columns:
        table['bid_premia'] = np.nan
        table['ask_premia'] = np.nan

    table[['last_reference', 'bid_reference', 'ask_reference', 'hedge_bid_price', 'hedge_ask_price', 'bid_guide', 'ask_guide', 'bid_premia', 'ask_premia']] = table[['last_reference', 'bid_reference', 'ask_reference', 'hedge_bid_price', 'hedge_ask_price', 'bid_guide', 'ask_guide', 'bid_premia', 'ask_premia']].ffill()

    if hedge is not None:
        table = table[table['hedge_bid_price'] == table['hedge_bid_price']]

    return edit_maker(table, parameters)

def edit_maker(table, parameters, delays=None):
    # Apply premia -------------------------------------------------------------
    premia = parameters.get('premia')
    if not table['bid_premia'].isnull().values.all():
        table['open_bid_price'] = table['bid_reference'] * (1 - table['bid_premia'])
        table['close_bid_price'] = table['bid_reference'] * (1 - table['bid_premia'])

        table['open_ask_price'] = table['ask_reference'] * (1 + table['ask_premia'])
        table['close_ask_price'] = table['ask_reference'] * (1 + table['ask_premia'])
    else:
        open_premia = parameters.get('open_premia')
        close_premia = parameters.get('close_premia')

        open_premia = open_premia if open_premia else premia
        close_premia = close_premia if close_premia else open_premia

        table['open_bid_price'] = table['bid_reference'] * (1 - open_premia)
        table['close_bid_price'] = table['bid_reference'] * (1 - close_premia)

        table['open_ask_price'] = table['ask_reference'] * (1 + open_premia)
        table['close_ask_price'] = table['ask_reference'] * (1 + close_premia)

    # Apply guide --------------------------------------------------------------
    table.loc[table['open_bid_price'] > table['bid_guide'], 'open_bid_price'] = table.loc[table['open_bid_price'] > table['bid_guide'], 'bid_guide']
    table.loc[table['close_bid_price'] > table['bid_guide'], 'close_bid_price'] = table.loc[table['close_bid_price'] > table['bid_guide'], 'bid_guide']

    table.loc[table['open_ask_price'] < table['ask_guide'], 'open_ask_price'] = table.loc[table['open_ask_price'] < table['ask_guide'], 'ask_guide']
    table.loc[table['close_ask_price'] < table['ask_guide'], 'close_ask_price'] = table.loc[table['close_ask_price'] < table['ask_guide'], 'ask_guide']

    table[['open_bid_price', 'close_bid_price', 'open_ask_price', 'close_ask_price']] = table[['open_bid_price', 'close_bid_price', 'open_ask_price', 'close_ask_price']]

    return table
