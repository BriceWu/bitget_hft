#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
from zmqfsi.service.zm_base import ZMBase
from zmqfsi.util.zm_env import RunEnv
import zmqfsi.util.zm_log as zm_log



class VolumeMonitor(ZMBase):
    def __init__(self, symbol):
        ZMBase.__init__(self)
        self._symbol = symbol
        self._logger = zm_log.get_log(self._symbol)
        self._host_address = "fapi.binance.com"


if __name__ == '__main__':
    RunEnv.set_run_env('test')
    _symbol = "doge_usdt"
    b = VolumeMonitor(_symbol)
    b.get_continuous_klines()
