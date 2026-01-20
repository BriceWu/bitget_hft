#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
from zmqfsi.service.zm_base import ZMBase
from zmqfsi.util.zm_env import RunEnv
import zmqfsi.util.zm_log as zm_log
from api.bn_public_rest_api import BinancePublicPerpApi



class VolumeMonitor(ZMBase):
    def __init__(self, symbol, volume_rate):
        """
        初始化
        :param symbol: 交易对
        :param volume_rate: 量比
        """
        ZMBase.__init__(self)
        self._symbol = symbol
        self._logger = zm_log.get_log(self._symbol)
        self._rest_api = None
        self._volume_rate = volume_rate

    def init_params(self):
        self._rest_api = BinancePublicPerpApi(self._symbol, self._logger)

    def start(self):
        pass


if __name__ == '__main__':
    RunEnv.set_run_env('test')
    _symbol = "doge_usdt"
    b = VolumeMonitor(_symbol)
    b.get_continuous_klines()
