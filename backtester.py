# Asimov -----------------------------------------------------------------------
from ..database.database import Database
from ..research.research import Research
from copy import deepcopy, copy
from datetime import datetime, timedelta
from time import time
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import json
import pytz
import gc

class Backtester:
    def __init__(self, blueprint):
        # Backtest Parameters --------------------------------------------------
        self.blueprint = blueprint
        # Identification -------------------------------------------------------
        self.actor = None
        self.pair = None
        self.exchange = None
        self.identify()
        # Research Parameters --------------------------------------------------
        self.research = self.initialize_research()
        # Data -----------------------------------------------------------------
        self.database = asimov.Database()
        self.table = self.get_table()

    def identify(self):
        self.actor = self.blueprint['module']
        self.pair = self.blueprint['params']['pair']
        self.exchange = self.blueprint['params']['interface']['params']['exchange']

    def initialize_research(self):
        return Research(self.actor, self.exchange, self.pair, json.dumps(self.blueprint))

    def get_table(self):
        if self.blueprint['module'].lower() == 'taker':
            data = self.database.load_trades()
        return data

    def trade_pair(self, pair, load_previous=True):
        # LOAD OLDER RESULTS ==================================================
        results = self.research.fetch_analysis(pair, self.since, self.until[pair], self.parameters[pair], fetch_data=fetch_data)
        if results != False and load_previous:
            return results

        tm = asimov.TradeManager(self.target.lower())
        column_map = {self.data[pair].columns[i]: i for i in range(len(self.data[pair].columns))}
        data_np = self.data[pair].values

        op = 0  # 0: se fora de trade | 1: se long | -1: se short
        i = 1
        while i < data_np.shape[0]:
            if op == 0:
                price = 1
                tm.insert_trade(i, price, 1, 'taker')
                op = 1
                break

            elif op == -1:
                price = 2
                tm.insert_trade(i, price, 1, 'maker')
                op = 0
            i += 1

        # ==========================================================================
        # Apuração de resultado
        tm.evolve_to_df(self.data_dict[pair])
        results = self.research.analyze_trades(self.data, tm.df_trades)

        self.research.save_results(results)
        return results
