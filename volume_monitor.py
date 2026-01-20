#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
from zmqfsi.service.zm_base import ZMBase
from zmqfsi.util.zm_env import RunEnv
import zmqfsi.util.zm_log as zm_log



class VolumeMonitor(ZMBase):
    def __init__(self, symbol, logger):
        ZMBase.__init__(self)
        self._symbol = symbol
        self._logger = logger
        if logger is None:
            raise Exception("日志对象未初始化")
        self._host_address = "fapi.binance.com"
        self._format_symbol = self.format_symbol()
        self._https_client = None
        self.init_https_connection()

if __name__ == '__main__':
    RunEnv.set_run_env('test')
    _symbol = "doge_usdt"
    _logger = zm_log.get_log(_symbol)
    b = VolumeMonitor(_symbol, _logger)
    b.get_continuous_klines()
