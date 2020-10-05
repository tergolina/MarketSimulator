from .builder.builder import Builder
from time import time
from datetime import datetime
from scipy.ndimage.interpolation import shift
import pandas as pd
import numpy as np
import gc


class Simulator:
    def __init__(self, parameters):
        self.builder = Builder(parameters)
        self.matrix = None
        self.table = None
        self.x = None
        self.bet_sizes = None
        self.bet_volume = None
        self.last_parameters = {}
        self.parameters = {}
        # Results --------------------------------------------------------------
        self.trades = {}
        self.results = {}

    def simplify(self):
        self.full_table['hedge_price'] = np.nan
        self.full_table['signal'] = 0
        self.full_table['quantity'] = 0
        if ('open_bid_price' in self.full_table.columns) and ('sell_price' in self.full_table.columns):
            self.full_table.loc[self.full_table['close_bid_price'] > self.full_table['sell_price'], 'signal'] = 1
            self.full_table.loc[self.full_table['open_bid_price'] > self.full_table['sell_price'], 'signal'] = 2
            self.full_table.loc[self.full_table['close_ask_price'] < self.full_table['buy_price'], 'signal'] = -1
            self.full_table.loc[self.full_table['open_ask_price'] < self.full_table['buy_price'], 'signal'] = -2

            self.full_table['target_price'] = self.full_table['buy_price'].fillna(value=0) + self.full_table['sell_price'].fillna(value=0)
            self.full_table.loc[self.full_table['signal'] == 1, 'target_price'] = self.full_table.loc[self.full_table['signal'] == 1, 'close_bid_price']
            self.full_table.loc[self.full_table['signal'] == 2, 'target_price'] = self.full_table.loc[self.full_table['signal'] == 2, 'open_bid_price']
            self.full_table.loc[self.full_table['signal'] == -1, 'target_price'] = self.full_table.loc[self.full_table['signal'] == -1, 'close_ask_price']
            self.full_table.loc[self.full_table['signal'] == -2, 'target_price'] = self.full_table.loc[self.full_table['signal'] == -2, 'open_ask_price']

            self.full_table.loc[(self.full_table['signal'] >= -2) & (self.full_table['signal'] < 0), 'quantity'] = self.full_table.loc[(self.full_table['signal'] >= -2) & (self.full_table['signal'] < 0), 'buy_quantity']
            self.full_table.loc[(self.full_table['signal'] > 0) & (self.full_table['signal'] <= 2), 'quantity'] = self.full_table.loc[(self.full_table['signal'] > 0) & (self.full_table['signal'] <= 2), 'sell_quantity']
        else:
            self.full_table['open_bid_price'] = np.nan
            self.full_table['close_bid_price'] = np.nan
            self.full_table['open_ask_price'] = np.nan
            self.full_table['close_ask_price'] = np.nan

        if ('open_buy_price' in self.full_table.columns) and ('ask_price' in self.full_table.columns):
            self.full_table.loc[self.full_table['close_buy_price'] > self.full_table['ask_price'], 'signal'] = 3
            self.full_table.loc[self.full_table['open_buy_price'] > self.full_table['ask_price'], 'signal'] = 4
            self.full_table.loc[self.full_table['close_sell_price'] < self.full_table['bid_price'], 'signal'] = -3
            self.full_table.loc[self.full_table['open_sell_price'] < self.full_table['bid_price'], 'signal'] = -4

            self.full_table['target_price'] = (self.full_table['bid_price'] + self.full_table['ask_price']) / 2
            self.full_table.loc[self.full_table['signal'] == 3, 'target_price'] = self.full_table.loc[self.full_table['signal'] == 3, 'ask_price']
            self.full_table.loc[self.full_table['signal'] == 4, 'target_price'] = self.full_table.loc[self.full_table['signal'] == 4, 'ask_price']
            self.full_table.loc[self.full_table['signal'] == -3, 'target_price'] = self.full_table.loc[self.full_table['signal'] == -3, 'bid_price']
            self.full_table.loc[self.full_table['signal'] == -4, 'target_price'] = self.full_table.loc[self.full_table['signal'] == -4, 'bid_price']

            self.full_table.loc[self.full_table['signal'] <= -3, 'quantity'] = self.full_table.loc[self.full_table['signal'] <= -3, 'bid_quantity']
            self.full_table.loc[self.full_table['signal'] >= 3, 'quantity'] = self.full_table.loc[self.full_table['signal'] >= 3, 'ask_quantity']
        else:
            self.full_table['open_buy_price'] = np.nan
            self.full_table['close_buy_price'] = np.nan
            self.full_table['open_sell_price'] = np.nan
            self.full_table['close_sell_price'] = np.nan

        self.full_table['hedge_price'] = (self.full_table['hedge_bid_price'] + self.full_table['hedge_ask_price']) / 2
        self.full_table.loc[self.full_table['signal'] > 0, 'hedge_price'] = self.full_table.loc[self.full_table['signal'] > 0, 'hedge_bid_price']
        self.full_table.loc[self.full_table['signal'] < 0, 'hedge_price'] = self.full_table.loc[self.full_table['signal'] < 0, 'hedge_ask_price']

        return self.full_table[self.full_table['signal'] != 0][['signal', 'target_price', 'hedge_price', 'quantity']]

    def __update_table(self):
        self.full_table = self.builder.get(self.x)
        self.table = self.simplify()
        self.matrix = self.table.values
        self.lenght = len(self.matrix)

    def __update_parameters(self, parameters):
        self.parameters.setdefault(self.x, {})
        self.last_parameters[self.x] = self.parameters[self.x].copy()
        self.parameters[self.x].update(parameters)

    def __update_bet_size(self):
        if self.__changed('bet_volume'):
            self.bet_quantity = None
            self.bet_volume = self.parameters[self.x]['bet_volume']
            self.bet_sizes = self.bet_volume / self.matrix[:,1]
        elif self.__changed('bet_quantity'):
            self.bet_volume = None
            self.bet_quantity = self.parameters[self.x]['bet_quantity']
            self.bet_sizes = np.full(self.lenght, self.parameters[self.x]['bet_quantity'])
        elif 'bet_quantity' in self.parameters[self.x]:
            self.bet_volume = None
            self.bet_quantity = self.parameters[self.x]['bet_quantity']
            self.bet_sizes = np.full(self.lenght, self.parameters[self.x]['bet_quantity'])
        elif 'bet_volume' in self.parameters[self.x]:
            self.bet_quantity = None
            self.bet_volume = self.parameters[self.x]['bet_volume']
            self.bet_sizes = self.bet_volume / self.matrix[:,1]

    def __changed(self, key):
        if key in self.parameters[self.x]:
            if key in self.last_parameters[self.x]:
                if self.parameters[self.x][key] != self.last_parameters[self.x][key]:
                    return True
            else:
                return True
        return False

    def supports_leverage(self):
        LEVERAGEBLE = {'kraken': ['XRP/USD', 'XRP/EUR', 'ETH/USD', 'ETH/EUR', 'ETH/BTC', 'BTC/USD', 'BTC/EUR'],
                       'bitmex': None,
                       'bitfinex': None}
        pair = self.parameters[self.x]['pair']
        exchange = self.parameters[self.x]['exchange']
        return (exchange in LEVERAGEBLE) and ((LEVERAGEBLE[exchange] is None) or (pair in LEVERAGEBLE[exchange]))

    def __has_hedge(self):
        return (self.__get_hedge_exchange() is not None)

    def load(self, x):
        self.x = x
        self.__update_parameters(self.builder.find(self.x)['parameters'])
        self.__update_table()

    def edit(self, parameters):
        self.__update_parameters(parameters)
        for item in self.parameters[self.x]:
            if item not in ['bet_volume', 'bet_quantity']:
                if self.__changed(item):
                    self.builder.edit(self.x, self.parameters[self.x])
                    self.__update_table()
                    break

    def run(self, x=None, parameters=None, initial_position=0):
        # Load -----------------------------------------------------------------
        if (x is not None) and (x != self.x):
            self.load(x)

        if parameters:
            self.edit(parameters)

        self.__update_bet_size()

        leveraged = self.supports_leverage()

        # Iterate --------------------------------------------------------------
        trades = np.zeros((self.lenght, 4))
        position = initial_position
        i = 0
        while i < self.lenght:
            if self.matrix[i][0] < 0:
                quantity = 0
                if leveraged:
                    if position > - self.bet_sizes[i]:
                        quantity = min(self.bet_sizes[i] + position, self.matrix[i][3])
                else:
                    if position > 0:
                        quantity = min(position, self.matrix[i][3])
                if quantity > 0:
                    position -= quantity
                    trades[i][0] = self.matrix[i][1]
                    trades[i][1] = self.matrix[i][2]
                    trades[i][2] = - quantity
                    if self.matrix[i][0] > -3:
                        trades[i][3] = 1
            elif self.matrix[i][0] > 0:
                if position < self.bet_sizes[i]:
                    quantity = min(self.bet_sizes[i] - position, self.matrix[i][3])
                    position += quantity
                    trades[i][0] = self.matrix[i][1]
                    trades[i][1] = self.matrix[i][2]
                    trades[i][2] = quantity
                    if self.matrix[i][0] < 3:
                        trades[i][3] = 1
            i += 1

        # Format ---------------------------------------------------------------
        self.trades[self.x] = pd.DataFrame(trades, index=self.table.index, columns=['target_price', 'hedge_price', 'quantity', 'maker'])
        self.trades[self.x] = self.trades[self.x][self.trades[self.x]['quantity'] != 0]
        return self.trades[self.x]

    def run_map(self, parameter_x, values_x, parameter_y, values_y, fees=None):
        a = time()

        raw_map = {}
        for i in range(len(values_y)):
            for j in range(len(values_x)):
                aa = time()
                self.run(parameters={parameter_x: values_x[j], parameter_y: values_y[i]})
                bb = int((time() - aa) * 1000)
                print(datetime.now(), '- [ Simulator ] Simulation generated in', bb, 'ms')

                aa = time()
                analysis = self.analysis(fees=fees)['combined']
                bb = int((time() - aa) * 1000)
                print(datetime.now(), '- [ Simulator ] Analysis generated in', bb, 'ms')

                for item in analysis:
                    raw_map.setdefault(item, [[0 for j in values_x] for i in values_y])
                    raw_map[item][i][j] = analysis[item]

        b = int((time() - a) * 1000)
        print(datetime.now(), '- [ Simulator ] Heatmap generated in', b, 'ms')

        return raw_map

    def format(self):
        raw = self.trades[self.x]

        df_target = pd.DataFrame()
        df_target['price'] = raw['target_price']
        df_target['quantity'] = raw['quantity']
        df_target['side'] = 'buy'
        df_target['side'][df_target['quantity'] < 0] = 'sell'
        df_target['quantity'] = abs(df_target['quantity'])
        df_target['exchange'] = self.__get_target_exchange()
        df_target['pair'] = self.__get_target_pair()

        d = {'target': df_target}

        if self.__has_hedge():
            df_hedge = pd.DataFrame()
            df_hedge['price'] = raw['hedge_price']
            df_hedge['quantity'] = raw['quantity']
            df_hedge['side'] = 'buy'
            df_hedge['side'][df_hedge['quantity'] > 0] = 'sell'
            df_hedge['quantity'] = abs(df_hedge['quantity'])
            df_hedge['exchange'] = self.__get_hedge_exchange()
            df_hedge['pair'] = self.__get_hedge_pair()

            d['hedge'] = df_hedge

        return d

    def insert_mark_price(self, df_trades):
        df_mark = self.full_table[['target_price', 'hedge_price']].astype('float64')
        df_mark = df_mark[(df_mark!=0).all(axis=1)]
        df_mark = df_mark.groupby(df_mark.index).last().dropna(how='all')
        df_trades = pd.concat([df_trades, df_mark], sort=False).sort_index(kind='mergesort')
        df_trades['quantity'] = df_trades['quantity'].fillna(value=0)
        df_trades = df_trades.ffill()
        df_trades = df_trades[df_trades['target_price'] == df_trades['target_price']]
        return df_trades

    def insert_costs(self, df_trades, fees):
        fees = self.__load_fees(fees)
        df_trades['target_fee'] = df_trades['maker'] * fees['target']['maker']
        df_trades['target_fee'][df_trades['maker'] == 0] = fees['target']['taker']
        df_trades['target_costs'] = abs(df_trades['quantity']) * df_trades['target_fee']
        df_trades['hedge_costs'] = abs(df_trades['quantity']) * fees['hedge']['taker']
        df_trades[['target_costs', 'hedge_costs']] = df_trades[['target_costs', 'hedge_costs']].fillna(value=0)

    def insert_change(self, df_trades):
        df_trades['target_change'] = ((df_trades['target_price'] / df_trades['target_price'].shift(1)) - 1).fillna(value=0)
        df_trades['hedge_change'] = ((df_trades['hedge_price'] / df_trades['hedge_price'].shift(1)) - 1).fillna(value=0)

    def __load_fees(self, fees):
        if fees is None:
            return {'target': self.__get_default_fees(self.__get_target_exchange()),
                    'hedge': self.__get_default_fees(self.__get_hedge_exchange())}
        elif fees == 0:
            return {'target': self.__get_lowest_fees(self.__get_target_exchange()),
                    'hedge': self.__get_lowest_fees(self.__get_hedge_exchange())}
        else:
            return fees

    def __get_target_exchange(self):
        return self.parameters[self.x].get('exchange')

    def __get_target_pair(self):
        return self.parameters[self.x].get('pair')

    def __get_hedge_exchange(self):
        hedge = self.parameters[self.x].get('hedge')
        if hedge is not None:
            parameters = self.builder.find(hedge)['parameters']
            return parameters.get('exchange')

    def __get_hedge_pair(self):
        hedge = self.parameters[self.x].get('hedge')
        if hedge is not None:
            parameters = self.builder.find(hedge)['parameters']
            return parameters.get('pair')

    def __get_default_fees(self, exchange):
        fees = {'maker': 0, 'taker': 0}
        if exchange == 'bitmex':
            fees['maker'] = -0.025 / 100
            fees['taker'] = 0.075 / 100
        elif exchange == 'binance':
            fees['maker'] = 0.054 / 100
            fees['taker'] = 0.054 / 100
        elif exchange == 'bitfinex':
            fees['maker'] = 0.1 / 100
            fees['taker'] = 0.2 / 100
        elif exchange == 'hitbtc':
            fees['maker'] = -0.01 / 100
            fees['taker'] = 0.1 / 100
        elif exchange == 'kraken':
            fees['maker'] = 0.16 / 100
            fees['taker'] = 0.20 / 100
        elif exchange == 'cex':
            fees['maker'] = 0.0 / 100
            fees['taker'] = 0.1 / 100
        return fees

    def __get_lowest_fees(self, exchange):
        fees = {'maker': 0, 'taker': 0}
        if exchange == 'bitmex':
            fees['maker'] = -0.025 / 100
            fees['taker'] = 0.075 / 100
        elif exchange == 'binance':
            fees['maker'] = 0.035 / 100
            fees['taker'] = 0.035 / 100
        elif exchange == 'bitfinex':
            fees['maker'] = 0.0 / 100
            fees['taker'] = 0.1 / 100
        elif exchange == 'hitbtc':
            fees['maker'] = -0.01 / 100
            fees['taker'] = 0.1 / 100
        elif exchange == 'kraken':
            fees['maker'] = 0.001 / 100
            fees['taker'] = 0.1 / 100
        elif exchange == 'cex':
            fees['maker'] = 0.0 / 100
            fees['taker'] = 0.1 / 100
        return fees

    def get_premia(self):
        premia = self.parameters[self.x].get('premia')
        if premia is None:
            premia = self.parameters[self.x].get('premium')
            if premia is None:
                premia = self.parameters[self.x].get('open_premium')
                if premia is None:
                    premia = self.parameters[self.x].get('open_premia')
        if isinstance(premia, str):
            premia = 1
        return premia

    def curve(self, fees=None):
        df_trades = self.trades[self.x]

        df_trades = self.insert_mark_price(df_trades)
        self.insert_costs(df_trades, fees)
        self.insert_change(df_trades)

        df_trades['target_position'] = df_trades['quantity'].cumsum().shift(1).fillna(value=0)
        df_trades['target_gains'] = (df_trades['target_position'] * df_trades['target_change'] - df_trades['target_costs']) * df_trades['target_price']
        df_trades['target_curve'] = df_trades['target_gains'].cumsum()

        df_trades['hedge_position'] = - df_trades['quantity'].cumsum().shift(1).fillna(value=0)
        df_trades['hedge_gains'] = (df_trades['hedge_position'] * df_trades['hedge_change'] - df_trades['hedge_costs']) * df_trades['hedge_price']
        df_trades['hedge_curve'] = df_trades['hedge_gains'].cumsum()

        df_trades['combined_curve'] = df_trades['hedge_curve'] + df_trades['target_curve']

        return {'market': df_trades['target_price'],
                'target': df_trades['target_curve'],
                'hedge': df_trades['hedge_curve'],
                'combined': df_trades['combined_curve']}

    def analysis(self, fees=None, relative_returns=True):
        premia = self.get_premia()
        leveraged = self.supports_leverage()
        bet_volume = self.parameters[self.x].get('bet_volume', 0)
        df_trades = self.trades[self.x]

        df_trades = self.insert_mark_price(df_trades)
        self.insert_costs(df_trades, fees)
        self.insert_change(df_trades)

        df_trades['date'] = df_trades.index.date
        df_trades['date_change'] = False
        df_trades.loc[df_trades['date'] != df_trades['date'].shift(1), 'date_change'] = True

        ar_trades = df_trades[['quantity', 'target_price', 'target_change', 'target_costs', 'hedge_price', 'hedge_change', 'hedge_costs', 'date_change']].values
        quantity = ar_trades[:,0].astype('float64')
        target_price = ar_trades[:,1].astype('float64')
        target_change = ar_trades[:,2].astype('float64')
        target_costs = ar_trades[:,3].astype('float64')
        date_change = ar_trades[:,-1].astype('bool')
        absolute_change = (target_price[-1] / target_price[0]) - 1

        # Target ---------------------------------------------------------------
        target_position = shift(quantity.cumsum(), 1, cval=0)
        target_gains = (target_position * target_change  - target_costs) * target_price
        target_daily_gains = target_gains.cumsum()[date_change]
        target_daily_gains = target_daily_gains - shift(target_daily_gains, 1, cval=0)
        if relative_returns and not leveraged:
            target_daily_gains = target_daily_gains - (absolute_change / (2 * len(target_daily_gains))) # (((1 + (absolute_change/2)) ** (1/len(target_daily_gains))) - 1)

        target_total_volume = abs(target_price * quantity).sum()
        target_total_gains = target_gains.sum()
        target_total_returns = (target_total_gains / bet_volume) if bet_volume != 0  else None
        target_sharpe = target_daily_gains.mean() / target_daily_gains.std()
        target_mean_returns = target_total_gains / target_total_volume


        if relative_returns and not leveraged:
            target_sharpe = target_daily_gains.mean() / target_daily_gains.std()
            target_mean_returns = (target_total_gains - (absolute_change * bet_volume / 2)) / target_total_volume if bet_volume != 0  else None
            target_total_returns = ((target_total_gains - (absolute_change * bet_volume / 2)) / bet_volume) if bet_volume != 0  else None

        target_results = {'sharpe': target_sharpe * ((252) ** (1/2)),
                          'mean_returns': target_mean_returns,
                          'efficiency': target_mean_returns / premia,
                          'total_gains': target_total_gains,
                          'total_volume': target_total_volume,
                          'total_returns': target_total_returns}

        # Combined -------------------------------------------------------------
        if self.__has_hedge():
            hedge_price = ar_trades[:,4].astype('float64')
            hedge_change = ar_trades[:,5].astype('float64')
            hedge_costs = ar_trades[:,6].astype('float64')

            hedge_position = shift(-quantity.cumsum(), 1, cval=0)
            combined_gains = target_gains + (hedge_position * hedge_change - hedge_costs) * hedge_price
            combined_daily_gains = combined_gains.cumsum()[date_change]
            combined_daily_gains = combined_daily_gains - shift(combined_daily_gains, 1, cval=0)

            combined_total_volume = target_total_volume + abs(hedge_price * quantity).sum()
            combined_total_gains = combined_gains.sum()
            combined_sharpe = combined_daily_gains.mean() / combined_daily_gains.std()
            combined_mean_returns = combined_total_gains / combined_total_volume

            combined_results = {'sharpe': combined_sharpe * ((252) ** (1/2)),
                                'mean_returns': combined_mean_returns,
                                'efficiency': combined_mean_returns / premia,
                                'total_gains': combined_total_gains,
                                'total_volume': combined_total_volume}
        else:
            combined_results = target_results

        self.results[self.x] = {'target': target_results, 'combined': combined_results}
        return self.results[self.x]
