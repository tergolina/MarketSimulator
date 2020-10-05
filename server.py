# Asimov -----------------------------------------------------------------------
from ..database.database import Database
# from asimov import Database
from time import time
import pandas as pd
from copy import copy, deepcopy
import numpy as np


'''
blueprint = {'trade': {'binance':  ['XRP/BTC', 'ETH/BTC'],
                       'hitbtc':   ['XRP/BTC', 'XRP/USDT', 'BTC/USDT']},

             'quote': {'binance':  ['XRP/BTC', 'EOS/BTC',  'EOS/USDT', 'BTC/USDT'],
                       'bitfinex': ['BTC/EUR', 'ETH/EUR',  'EOS/USD',  'LTC/BTC'],
                       'bitmex':   ['ETHUSD',  'XBTUSD']},

             'account': {'bitfinex': ['ETH/EUR', 'ETH/BTC', 'EOS/EUR']},

             'misc':  [{'function': vol_1min,
                       'name': 'vol',
                       'args': ['bitmex', 'XBTUSD', 20]}],

             'since': '2018-12-13',

             'delay': {'binance': {'trade': 70, 'quote': 70, 'account': 100},
                       'bitfinex': {'trade': 100, 'quote': 0, 'account': 100},
                       'bitmex': {'trade': 1000, 'quote': 1000, 'account': 1000},
                       'hitbtc': {'trade': 250, 'quote': 250, 'account': 100}}}
'''
class Server:
    def __init__(self, blueprint, subscriber):
        self.blueprint = blueprint
        self.subscriber = subscriber
        self.exchanges = self.get_exchanges()
        self.load_table()
        # Initialize variables --------------------------------------------------
        self.initialize_control()

    def get_exchanges(self):
        exchanges = []
        for item in ['maker', 'taker', 'quote', 'trade']:
            exchanges += list(self.blueprint[item].keys())
        return list(set(exchanges))

    # Download and treat data ---------------------------------------------------
    def filter_blueprint(self, blueprint):
        bp = deepcopy(blueprint)
        if ('maker' in bp) and ('trade' in bp):
            for exchange in bp['maker']:
                for pair in bp['maker'][exchange]:
                    if (exchange in bp['trade']) and (pair in bp['trade'][exchange]):
                        bp['trade'][exchange].remove(pair)
        if ('taker' in bp) and ('quote' in bp):
            for exchange in bp['taker']:
                for pair in bp['taker'][exchange]:
                    if (exchange in bp['quote']) and (pair in bp['quote'][exchange]):
                        bp['quote'][exchange].remove(pair)
        return bp

    def format_table(self):
         self.df_table['notify'] = False
         for item in ['trade', 'quote']:
             for exchange in self.blueprint[item]:
                 for pair in self.blueprint[item][exchange]:
                     self.df_table.loc[(self.df_table['exchange'] == exchange) & (self.df_table['pair'] == pair), 'notify'] = True

    def add_columns_to_df(self, df):
        columns = ['bid', 'bid_quantity', 'ask', 'ask_quantity', 'price', 'side', 'quantity']
        for item in columns:
            if not item in df:
                df[item] = np.nan
        return df

    def load_table(self):
        filtered_bp = self.filter_blueprint(self.blueprint)
        if not 'until' in filtered_bp:
            filtered_bp['until'] = None
        self.database = Database()
        dfs = []
        if 'maker' in filtered_bp and filtered_bp['maker']:
            df = self.database.fetch_trades(filtered_bp['maker'], filtered_bp['since'], end_date=filtered_bp['until'], infer_book=False, residual_sync=True, change_only=False)
            dfs += [self.add_columns_to_df(df)]
        if 'trade' in filtered_bp and filtered_bp['trade']:
            df = self.database.fetch_trades(filtered_bp['trade'], filtered_bp['since'], end_date=filtered_bp['until'], infer_book=False, residual_sync=True, change_only=True)
            dfs += [self.add_columns_to_df(df)]
        if 'taker' in filtered_bp and filtered_bp['taker']:
            df = self.database.fetch_book(filtered_bp['taker'], filtered_bp['since'], end_date=filtered_bp['until'], residual_sync=True)
        if 'quote' in filtered_bp and filtered_bp['quote']:
            df = self.database.fetch_book(filtered_bp['quote'], filtered_bp['since'], end_date=filtered_bp['until'], residual_sync=True)
            dfs += [self.add_columns_to_df(df)]
        df = df[~df.index.duplicated(keep='first')]
        self.df_table = pd.concat(dfs, sort=True).sort_index()
        self.format_table()
        self.calculate_misc()
        self.ar_table = self.df_table.values

    def calculate_misc(self):
        self.misc = {}
        self.misc_map = {}
        if 'misc' in self.blueprint:
            misc_column_names = []
            for misc in self.blueprint['misc']:
                self.df_table = misc['function'](self.df_table, misc['name'], *misc['args'])
                misc_column_names += [misc['name']]
            self.misc_map = {self.df_table.columns[i]: i for i in range(len(self.df_table.columns)) if self.df_table.columns[i] in misc_column_names}
            for misc in self.misc_map:
                self.misc[misc] = 0

    # Initialize --------------------------------------------------------------
    def subscribe(self, subscriber):
        self.subscriber = subscriber
        self.initialize_control()

    def initialize_marketdata(self):
        md = {}
        if 'trade' in self.blueprint:
            for exchange in self.blueprint['trade']:
                for pair in self.blueprint['trade'][exchange]:
                    if exchange not in md:
                        md[exchange] = {}
                    if pair not in md[exchange]:
                        md[exchange][pair] = {}
                    if 'last' not in md[exchange][pair]:
                        md[exchange][pair]['last'] = None
                    if 'side' not in md[exchange][pair]:
                        md[exchange][pair]['side'] = None
        if 'quote' in self.blueprint:
            for exchange in self.blueprint['quote']:
                for pair in self.blueprint['quote'][exchange]:
                    if exchange not in md:
                        md[exchange] = {}
                    if pair not in md[exchange]:
                        md[exchange][pair] = {}
                    if 'bid' not in md[exchange][pair]:
                        md[exchange][pair]['bid'] = None
                    if 'ask' not in md[exchange][pair]:
                        md[exchange][pair]['ask'] = None
        return md

    def initialize_position(self):
        position = {}
        for item in ['maker', 'taker']:
            if item in self.blueprint:
                for exchange in self.blueprint[item]:
                    for pair in self.blueprint[item][exchange]:
                        if exchange not in position:
                            position[exchange] = {}
                        if pair not in position[exchange]:
                            position[exchange][pair] = 0
        return position

    def initialize_control(self):
        # Marketdata -----------------------------------------------------------
        self.marketdata = self.initialize_marketdata()
        # Account --------------------------------------------------------------
        self.orders = []
        self.position = self.initialize_position()
        self.activated_orders = []
        self.queue = []
        # Control Map ----------------------------------------------------------
        self.map = {self.df_table.columns[i]: i for i in range(len(self.df_table.columns))}
        self.TIMESTAMP = self.map['timestamp']
        self.EXCHANGE = self.map['exchange']
        self.PRICE = self.map['pair']
        self.SIDE = self.map['side']
        self.PRICE = self.map['price']
        self.QUANTITY = self.map['quantity']
        self.PAIR = self.map['pair']
        self.BID = self.map['bid']
        self.ASK = self.map['ask']
        self.BID_QUANTITY = self.map['bid_quantity']
        self.ASK_QUANTITY = self.map['ask_quantity']
        self.NOTIFY = self.map['notify']
        # Control info ---------------------------------------------------------
        self.i = 0
        # Basic info -----------------------------------------------------------
        self.timestamp = None
        self.received_timestamp = None
        self.exchange = None
        self.pair = None
        self.notify_marketdata = True
        self.notify_maker = False
        self.notify_taker = False
        self.notify_orders = True
        self.delay = self.blueprint['delay']
        # Trade info -----------------------------------------------------------
        self.side = None
        self.price = None
        self.quantity = None
        # Quote info -----------------------------------------------------------
        self.bid = None
        self.ask = None
        self.bid_quantity = None
        self.ask_quantity = None
        # Account info ---------------------------------------------------------
        self.order_id = 0
        self.last_timestamp = {ex: {'maker' : 0, 'taker' : 0, 'current' : 0} for ex in self.exchanges}

    # ==========================================================================
    # Run server  --------------------------------------------------------------
    def next_step(self):
        # Update current ticker ------------------------------------------------
        self.notify_marketdata = self.ar_table[self.i, self.NOTIFY]
        self.timestamp = self.ar_table[self.i, self.TIMESTAMP]
        self.exchange = self.ar_table[self.i, self.EXCHANGE]
        self.pair = self.ar_table[self.i, self.PAIR]
        # Trade ----------------------------------------------------------------
        self.price = self.ar_table[self.i, self.PRICE]
        self.quantity = self.ar_table[self.i, self.QUANTITY]
        self.side = self.ar_table[self.i, self.SIDE]
        # Quote ----------------------------------------------------------------
        self.bid = self.ar_table[self.i, self.BID]
        self.bid_quantity = self.ar_table[self.i, self.BID_QUANTITY]
        self.ask = self.ar_table[self.i, self.ASK]
        self.ask_quantity = self.ar_table[self.i, self.ASK_QUANTITY]

        self.notify_maker = True if self.price and self.side and self.quantity else False
        self.notify_taker = True if self.bid and self.ask else False

        self.run_delay()

        if self.notify_marketdata:
            self.marketdata_to_queue()

        notify_orders_time = self.verify_orders()

        trades = []
        if self.notify_maker:
            trades += self.verify_maker_trades()
        if self.notify_taker:
            trades += self.verify_taker_trades()

        if trades:
            self.trades_to_queue(trades)
        elif self.notify_orders:
            self.orders_to_queue(notify_orders_time)

        self.notify()

        self.i += 1
        return self.i < len(self.ar_table)

    def notify(self):
        if len(self.queue) > 0 and self.timestamp >= self.queue[0][0]:
            deletion_list = []
            for i in range(len(self.queue)):
                item = self.queue[i]
                if self.timestamp >= item[0]:
                    deletion_list += [i]
                    self.received_timestamp = item[0]
                    self.subscriber(item[1])
            for i in deletion_list[::-1]:
                del self.queue[i]

    # Verifies -----------------------------------------------------------------
    def verify_orders(self):
        '''
            Se o cancelamento de alguma ordem já estiver sido agendado e o
            timestamp já tiver passado, ou se a quantidade restante da ordem
            estiver zerada deleta a ordem dos controles.
        '''
        notify_time = self.timestamp if self.i == 0 else None
        len_orders = len(self.orders)
        if len_orders > 0:
            deletion_list = []
            for i in range(len_orders):
                # Deleta a ordem
                if ((self.orders[i]['open']) and
                    (self.orders[i]['delete_at'] != None) and
                    (self.orders[i]['delete_at'] <= self.timestamp)) or (self.orders[i]['quantity'] <= 0):
                    self.notify_orders = True
                    if self.orders[i]['quantity'] <= 0:
                        notify_time = self.timestamp + self.get_read_delay(self.orders[i]['exchange'], 'account')
                    else:
                        notify_time = self.orders[i]['delete_at'] + self.get_read_delay(self.orders[i]['exchange'], 'account')
                    deletion_list += [i]
                # Ativa a ordem
                elif ((not self.orders[i]['open']) and (self.orders[i]['activate_at'] <= self.timestamp)):
                    self.notify_orders = True
                    notify_time = self.orders[i]['activate_at'] + self.get_read_delay(self.orders[i]['exchange'], 'account')
                    self.activated_orders += [self.orders[i]['id']]
                    self.orders[i]['open'] = True
                # Substitui a ordem
                elif len(self.orders[i]['replace']) > 0:
                    j = 0
                    while j < len(self.orders[i]['replace']):
                        if self.orders[i]['replace'][j][0] < self.timestamp:
                            self.notify_orders = True
                            notify_time = self.orders[i]['replace'][j][0] + self.get_read_delay(self.orders[i]['exchange'], 'account')
                            self.orders[i]['price'] = self.orders[i]['replace'][j][1]
                            self.orders[i]['replace'] = self.orders[i]['replace'][j+1:]
                            j-=1
                        j+=1
            for i in deletion_list[::-1]:
                del self.orders[i]
        return notify_time

    def verify_taker_trades(self):
        trades = []
        if (('taker' in self.blueprint)
        and (self.exchange in self.blueprint['taker'])
        and (self.pair in self.blueprint['taker'][self.exchange])):
            for order in self.orders:
                if ((order['open']) and
                   (order['exchange'] == self.exchange) and
                   (order['pair'] == self.pair)):
                    if order['side'] == 'sell':
                        if self.bid >= order['price']:
                            executed_quantity = abs(order['quantity'])
                            order['price'] = self.bid
                            order['quantity'] -= executed_quantity
                            order['volume'] = order['quantity'] * order['price']
                            trades += [{'timestamp': self.timestamp,
                                        'id': order['id'],
                                        'side' : order['side'],
                                        'exchange': order['exchange'],
                                        'pair': order['pair'],
                                        'price': order['price'],
                                        'quantity': executed_quantity,
                                        'volume': order['price'] * executed_quantity}]
                            self.position[self.exchange][self.pair] -= executed_quantity
                    elif order['side'] == 'buy':
                        if self.ask <= order['price']:
                            executed_quantity = abs(order['quantity'])
                            order['price'] = self.ask
                            order['quantity'] -= executed_quantity
                            order['volume'] = order['quantity'] * order['price']
                            trades += [{'timestamp': self.timestamp,
                                        'id': order['id'],
                                        'side' : order['side'],
                                        'exchange': order['exchange'],
                                        'pair': order['pair'],
                                        'price': order['price'],
                                        'quantity': executed_quantity,
                                        'volume': order['price'] * executed_quantity}]
                            self.position[self.exchange][self.pair] += executed_quantity
        return trades

    def verify_maker_trades(self):
        trades = []
        if (('maker' in self.blueprint)
        and (self.exchange in self.blueprint['maker'])
        and (self.pair in self.blueprint['maker'][self.exchange])):
            for order in self.orders:
                if ((order['open']) and
                   (order['exchange'] == self.exchange) and
                   (order['pair'] == self.pair)):
                    if (self.side == 'buy') and (order['side'] == 'sell'):
                        if self.price > order['price']:
                            executed_quantity = abs(min(order['quantity'], self.quantity))
                            self.quantity -= executed_quantity
                            order['quantity'] -= executed_quantity
                            order['volume'] = order['quantity'] * order['price']
                            trades += [{'timestamp': self.timestamp,
                                        'id': order['id'],
                                        'side' : order['side'],
                                        'exchange': order['exchange'],
                                        'pair': order['pair'],
                                        'price': order['price'],
                                        'quantity': executed_quantity,
                                        'volume': order['price'] * executed_quantity}]
                            self.position[self.exchange][self.pair] -= executed_quantity
                    elif (self.side == 'sell') and (order['side'] == 'buy'):
                        if self.price < order['price']:
                            executed_quantity = abs(min(order['quantity'], self.quantity))
                            self.quantity -= executed_quantity
                            order['quantity'] -= executed_quantity
                            order['volume'] = order['quantity'] * order['price']
                            trades += [{'timestamp': self.timestamp,
                                        'id': order['id'],
                                        'side' : order['side'],
                                        'exchange': order['exchange'],
                                        'pair': order['pair'],
                                        'price': order['price'],
                                        'quantity': executed_quantity,
                                        'volume': order['price'] * executed_quantity}]
                            self.position[self.exchange][self.pair] += executed_quantity
        return trades

    # Add to queue -------------------------------------------------------------
    def trades_to_queue(self, trades):
        update = {'exchange': self.exchange,
                  'event': 'trade',
                  'update': trades,
                  'account': {'orders' : self.order_list_to_standard(self.orders),
                              'position' : self.position},
                  'timestamp': self.last_timestamp[self.exchange]['current']}
        self.queue += [[self.last_timestamp[self.exchange]['current'], update]]
        self.queue.sort(key=lambda x: x[0])
        trades = []

    def orders_to_queue(self, notify_time):
        update = {'event': 'order',
                  'account': {'orders' : self.order_list_to_standard(self.orders),
                              'position' : self.position,
                              'activated_orders' : self.activated_orders},
                  'timestamp': self.last_timestamp[self.exchange]['current']}
        self.activated_orders = []
        if not notify_time:
            notify_time = self.last_timestamp[self.exchange]['current']
        self.queue += [[notify_time, update]]
        self.queue.sort(key=lambda x: x[0])
        self.notify_orders = False

    def marketdata_to_queue(self):
        if ('side' in self.marketdata[self.exchange][self.pair]) and ('last' in self.marketdata[self.exchange][self.pair]):
            self.marketdata[self.exchange][self.pair]['side'] = self.side
            self.marketdata[self.exchange][self.pair]['last'] = self.price
            event = 'last'
        elif ('bid' in self.marketdata[self.exchange][self.pair]) and ('ask' in self.marketdata[self.exchange][self.pair]):
            self.marketdata[self.exchange][self.pair]['bid'] = self.bid
            self.marketdata[self.exchange][self.pair]['ask'] = self.ask
            event = 'quote'
        else:
            return

        for misc in self.misc_map:
            self.misc[misc] = self.ar_table[self.i, self.misc_map[misc]]

        update = {'exchange': self.exchange,
                  'event': event,
                  'marketdata': self.marketdata,
                  'misc': self.misc,
                  'timestamp': self.last_timestamp[self.exchange]['current']}
        self.queue += [[self.last_timestamp[self.exchange]['current'], update]]
        self.queue.sort(key=lambda x: x[0])

    # Auxiliary ----------------------------------------------------------------
    def order_list_to_standard(self, order_list):
        orders = {}
        for order in order_list:
            exchange = order['exchange']
            pair = order['pair']
            book_side = 'bid' if order['side'] == 'buy' else 'ask'
            if exchange not in orders:
                orders[exchange] = {}
            if pair not in orders[exchange]:
                orders[exchange][pair] = {'bid': [], 'ask': []}
            orders[exchange][pair][book_side] += [order]
        return orders

    def get_read_delay(self, exchange, type):
        if type == 'maker':
            delay = self.delay[exchange]['trade']
        elif type == 'maker':
            delay = self.delay[exchange]['quote']
        else:
            delay = self.delay[exchange][type]
        return delay / 1000

    def get_write_delay(self, exchange):
        return (self.delay[exchange]['account'] / 2) / 1000

    def run_delay(self):
        if self.notify_maker:
            delay = self.get_read_delay(self.exchange, 'maker')
            self.last_timestamp[self.exchange]['maker'] = max(self.timestamp + delay, self.last_timestamp[self.exchange]['maker'])
            self.last_timestamp[self.exchange]['current'] = self.last_timestamp[self.exchange]['maker']
        elif self.notify_maker:
            delay = self.get_read_delay(self.exchange, 'taker')
            self.last_timestamp[self.exchange]['taker'] = max(self.timestamp + delay, self.last_timestamp[self.exchange]['taker'])
            self.last_timestamp[self.exchange]['current'] = self.last_timestamp[self.exchange]['taker']
        else:
            delay = self.get_read_delay(self.exchange, 'maker')
            self.last_timestamp[self.exchange]['maker'] = max(self.timestamp + delay, self.last_timestamp[self.exchange]['maker'])
            delay = self.get_read_delay(self.exchange, 'taker')
            self.last_timestamp[self.exchange]['taker'] = max(self.timestamp + delay, self.last_timestamp[self.exchange]['taker'])
            self.last_timestamp[self.exchange]['current'] = max(self.last_timestamp[self.exchange]['maker'], self.last_timestamp[self.exchange]['taker'])

    # Commands -----------------------------------------------------------------
    def place_order(self, exchange, pair, side, price, quantity):
        delay = self.get_write_delay(exchange)
        order = {'id': self.order_id,
                 'exchange': exchange,
                 'pair': pair,
                 'side': side,
                 'quantity': quantity,
                 'price': price,
                 'volume': price * quantity,
                 'activate_at': self.received_timestamp + delay,
                 'delete_at': None,
                 'open' : False,
                 'replace' : []}
        self.order_id += 1
        self.orders += [order]
        self.notify_orders = True

    def replace_order(self, order, price):
        for o in self.orders:
            if o['id'] == order['id'] and o['open']:
                delay = self.get_write_delay(o['exchange'])
                o['replace'] += [[self.received_timestamp + delay, price]]
        self.notify_orders = True

    def cancel_order(self, order):
        for o in self.orders:
            if o['id'] == order['id']:
                if o['open'] and o['delete_at'] == None:
                    o['delete_at'] = self.received_timestamp + self.get_write_delay(o['exchange'])
                break
        self.notify_orders = True

if __name__ == '__main__':
    blueprint = {'trade': {'binance':  ['XRP/USDT', 'XRP/BTC'],
                           'hitbtc':   ['XRP/BTC']},

                 # 'quote': {'binance':  ['XRP/BTC', 'EOS/BTC',  'EOS/USDT', 'BTC/USDT'],
                 #           'bitfinex': ['BTC/EUR', 'ETH/EUR',  'EOS/USD',  'LTC/BTC'],
                 #           'bitmex':   ['ETHUSD',  'XBTUSD']},

                 'quote': {},

                 'maker': {'binance': ['XRP/USDT']},

                 # 'taker': {'bitfinex': ['ETH/EUR', 'ETH/BTC', 'EOS/EUR']},

                 'taker': {},

                 'misc':  [{'function': twap,
                           'name': 'vol',
                           'args': ['XRP/USDT', 'binance', 'XRP/BTC', 'hitbtc', '10min']}],

                 'since': '2018-12-24',

                 'delay': {'binance': {'trade': 70, 'quote': 70, 'account': 100},
                           'hitbtc': {'trade': 250, 'quote': 250, 'account': 100}}}

    server = Server(blueprint, lambda x: print(x))
    server.place_order('binance', 'XRP/USDT', 'buy', 0.3850, 1)
    server.next_step()
