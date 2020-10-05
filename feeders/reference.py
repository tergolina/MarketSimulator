from ..new_utils.new_utils import *
from datetime import datetime
import pandas as pd
import numpy as np
import gc


def build_reference(tables, blueprint, delays=None):
    parameters = blueprint['parameters']
    relationship = parameters['relationship']
    get_method = parameters.get('get_method', {})
    group_method = parameters.get('group_method')

    result = get_reference(tables, relationship, get_method)
    return group(result, group_method)

def get_reference(tables, relationship, get_method):
    result = pd.DataFrame()
    if '+' in relationship:
        for x in relationship.split('+'):
            ref = get_reference(tables, x, get_method)
            result = combine_references(result, ref, '+')
    elif '/' in relationship:
        for x in relationship.split('/'):
            ref = get_reference(tables, x, get_method)
            result = combine_references(result, ref, '/')
    elif '*' in relationship:
        for x in relationship.split('*'):
            ref = get_reference(tables, x, get_method)
            result = combine_references(result, ref, '*')
    elif relationship in tables:
        result = apply(tables[relationship], get_method.get(relationship))
        result = result[~result.index.duplicated(keep='first')]
    return result.ffill().dropna()

def combine_references(result, ref, op):
    if result.empty:
        return ref
    else:
        # print (datetime.now(), '- [ Reference ] Combining references...')
        index = result.index.union(ref.index).drop_duplicates()
        result = result.reindex(index).sort_index().ffill()
        ref = ref.reindex(index).sort_index().ffill()

        if (op == '+') and ('bid' in result.columns) and ('ask' in result.columns) and ('ask' in ref.columns) and ('bid' in ref.columns):
            result['bid'] = pd.Series(np.minimum(result['bid'].values, ref['bid'].values), index=result.index)
            result['ask'] = pd.Series(np.maximum(result['ask'].values, ref['ask'].values), index=result.index)
        elif op == '/':
            if (list(result.columns) == list(ref.columns)) or (len(ref.columns) == 1):
                if ('bid' in ref.columns) and ('ask' in ref.columns):
                    ref = ref.iloc[:, ::-1]
                result = pd.DataFrame(result.values / ref.values, index=result.index, columns=result.columns)
            elif len(result.columns) == 1:
                result = pd.DataFrame(result.values / ref.values, index=result.index, columns=ref.columns)
        elif op == '*':
            if (list(result.columns) == list(ref.columns)) or (len(ref.columns) == 1):
                result = pd.DataFrame(result.values * ref.values, index=result.index, columns=result.columns)
            elif len(result.columns) == 1:
                result = pd.DataFrame(result.values * ref.values, index=result.index, columns=ref.columns)

        return result.ffill()

def apply(table, method):
    if method == 'mid':
        return (table[['bid']].rename(columns={'bid': 'mid'}) + table[['ask']].rename(columns={'ask': 'mid'})) / 2
    elif method == 'last':
        return table[['last']]
    elif ((method is not None) and (('book' in method) or ('quote' in method))) or ((method is None) and ('bid' in table.columns) and ('ask' in table.columns)):
        return table[['bid', 'ask']]
    else:
        return table
