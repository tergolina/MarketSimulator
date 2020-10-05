from .actor import build_actor
import pandas as pd
import numpy as np


def build_hedger(tables, blueprint, delays=None):
    return build_actor(tables, blueprint)[['bid_price', 'ask_price']].rename(columns={'bid_price': 'hedge_bid_price', 'ask_price': 'hedge_ask_price'})
