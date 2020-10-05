from ..new_utils.new_utils import *
from .feeder import build_data
from datetime import timezone
import pandas as pd
import numpy as np
import gc


def build_window(tables, blueprint, delays=None):
    parameters = blueprint['parameters']
    window = parameters.get('window')
    candle_size = parameters.get('candle_size', 60)

    data = build_data(tables, blueprint)

    data['timestamp'] = data.index.floor(str(candle_size) + 'S')
    data = data.groupby('timestamp').last()

    for column in data.columns:
        rw = rolling_window(data[column].values, window)
        data[column] = pd.DataFrame([{column: x} for x in rw], index=data.index[window-1:])[column]

    return data
