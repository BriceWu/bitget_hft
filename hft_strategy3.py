#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
import time
import asyncio
from hft_strategy2 import HFTStrategyTWO


class HFTStrategyTHREE(HFTStrategyTWO):
    def __init__(self, symbol, mark, volume_rate, trade_side):
        HFTStrategyTWO.__init__(self, symbol, mark, volume_rate, trade_side)
        self._stop_loss_rate = 0.0025
