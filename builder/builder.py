from ...database.database import Database2
from ..actors.taker import build_taker, edit_taker
from ..actors.maker import build_maker, edit_maker
from ..actors.hedger import build_hedger
from ..feeders.feeder import build_feeder
from ..feeders.window import build_window
from ..feeders.reference import build_reference
from ..feeders.premia import build_premia
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import gc


class Builder:
    def __init__(self, parameters):
        self.database = Database2()
        self.__identify_parameters(parameters)

        self.raw = self.get_raw_data()
        self.tables = self.build()

    def __identify_parameters(self, parameters):
        self.blueprint = parameters.get('blueprint')
        self.since = parameters.get('since')
        self.until = parameters.get('until')
        self.delays = parameters.get('delays')

    def __list_sources(self, blueprint=None):
        blueprint = self.blueprint if blueprint is None else blueprint
        sources = []
        for x in blueprint:
            sources += self.__list_sources(blueprint=blueprint[x].get('modules', {}))

            name = blueprint[x]['module']

            if name in ['Feeder', 'Window']:
                parameters = blueprint[x]['parameters']
                sources += [parameters]
            elif name in ['Maker']:
                parameters = blueprint[x]['parameters']
                parameters.update({'source': 'trade'})
                sources += [parameters]
            elif name in ['Taker', 'Hedger']:
                parameters = blueprint[x]['parameters']
                parameters.update({'source': 'book'})
                sources += [parameters]
        return sources

    def get_raw_data(self):
        sources = self.__list_sources()

        compass = {}
        for item in sources:
            compass.setdefault(item['exchange'], {})
            source = 'quote' if item['source'] == 'book' else item['source']
            compass[item['exchange']].setdefault(source, [])
            if item['pair'] not in compass[item['exchange']][source]:
                compass[item['exchange']][source] += [item['pair']]

        raw_data = self.database.fetch(compass, self.since, end_date=self.until, fetch_from_remote=True, residual_sync=1200)
        dfs = []
        for s in raw_data:
            if 'quantity' in raw_data[s]:
                aux = 'first'

                raw_data[s]['timestamp'] = raw_data[s].index
                buy_trades = raw_data[s][raw_data[s]['side'] == 'buy']
                sell_trades = raw_data[s][raw_data[s]['side'] == 'sell']

                buy_trades = buy_trades.sort_values('price').sort_values('timestamp', kind='mergesort')
                sell_trades = sell_trades.sort_values('price', ascending=False).sort_values('timestamp', kind='mergesort')
                del buy_trades['timestamp'], sell_trades['timestamp']
                gc.collect()
                #
                # buy_trades = buy_trades.groupby(buy_trades.index).agg({'quantity': 'sum', 'price': aux, 'side': aux, 'exchange': aux, 'pair': aux})
                # sell_trades = sell_trades.groupby(sell_trades.index).agg({'quantity': 'sum', 'price': aux, 'side': aux, 'exchange': aux, 'pair': aux})

                df = pd.concat([buy_trades, sell_trades], sort=False).sort_index(kind='mergesort')
            else:
                df = raw_data[s].groupby(raw_data[s].index).last()
            dfs += [self.__normalize_columns(df)]

        return pd.concat(dfs, sort=False).sort_index(kind='mergesort')

    def __normalize_columns(self, df):
        columns = ['bid', 'bid_quantity', 'ask', 'ask_quantity', 'price', 'side', 'quantity']
        for item in columns:
            if not item in df:
                df[item] = np.nan
        return df

    def has_dependancy(self, x, modules):
        if x in modules:
            return True
        else:
            for y in modules:
                if self.has_dependancy(x, modules[y].get('modules', {})):
                    break
            else:
                return True
            return False

    def delete(self, x, blueprint=None):
        blueprint = self.blueprint if blueprint is None else blueprint
        for y in blueprint:
            self.delete(x, blueprint[y].get('modules', {}))
            if (y == x) or self.has_dependancy(x, blueprint[y].get('modules', {})):
                del self.tables[y]
                gc.collect()

    def build(self, blueprint=None):
        blueprint = self.blueprint if blueprint is None else blueprint
        tables = {'raw': self.raw}

        for x in blueprint:
            tables.update(self.build(blueprint=blueprint[x].get('modules', {})))

            if x not in tables:
                name = blueprint[x]['module']
                # print (datetime.now(), '- [ Builder ] Building table for', x, name)
                tables[x] = self.identify(name)(tables, blueprint[x], delays=self.delays)
        return tables

    def identify(self, name, method='build'):
        if method == 'edit':
            if name == 'Maker':
                return edit_maker
            elif name == 'Taker':
                return edit_taker
        else:
            if name == 'Feeder':
                return build_feeder
            elif name == 'Window':
                return build_window
            elif name == 'Reference':
                return build_reference
            elif name == 'Premia':
                return build_premia
            elif name == 'Maker':
                return build_maker
            elif name == 'Taker':
                return build_taker
            elif name == 'Hedger':
                return build_hedger

    def find(self, x, blueprint=None):
        blueprint = self.blueprint if blueprint is None else blueprint
        for y in blueprint:
            module = self.find(x, blueprint=blueprint[y].get('modules', {}))
            if module:
                return module
            if y == x:
                return blueprint[x]

    def replace(self, x, parameters):
        module = self.find(x)
        if module:
            module['parameters'].update(parameters)
            self.blueprint[x] = module
        return module

    def edit(self, x, parameters):
        module = self.replace(x, parameters)
        if module:
            if module['module'] in ['Taker', 'Maker']:
                self.tables[x] = self.identify(module['module'], method='edit')(self.tables[x], module['parameters'])
            else:
                self.delete(x)
                self.build()

    def get(self, x):
        return self.tables[x].copy(deep=False)
