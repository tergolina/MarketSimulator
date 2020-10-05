from ..new_utils.new_utils import *
import pandas as pd
import numpy as np
import gc


def build_premia(tables, blueprint, delays=None):
    parameters = blueprint['parameters']

    x = parameters.get('x', 0)
    window = parameters.get('window', 1)
    reference_name = parameters.get('reference')
    minimum = parameters.get('minimum')

    df = tables[reference_name].copy()

    spread = ((df['ask'] / df['bid']) - 1).values

    rs = rolling_window(spread, window)
    df_premia = pd.DataFrame([{'bid': np.mean(i) + x * np.std(i)} for i in rs if i is not None], index=df.index[window-1:])
    df_premia.loc[df_premia['bid'] < minimum, 'bid'] = minimum
    df_premia['ask'] = df_premia['bid']
    return df_premia