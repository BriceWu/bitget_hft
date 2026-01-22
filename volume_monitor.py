#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
import traceback, socket, http.client, time, os, sys
from zmqfsi.service.zm_base import ZMBase
from zmqfsi.util.zm_env import RunEnv
import zmqfsi.util.zm_log as zm_log
from api.bn_public_rest_api import BinancePublicPerpApi

BASE_VOLUME_DICT = {
    'doge_usdt': 5 * 100 * 10000  # 5M
}



class VolumeMonitor(ZMBase):
    def __init__(self, symbol, volume_rate, trade_side):
        """
        初始化
        :param symbol: 交易对
        :param volume_rate: 量比
        :param trade_side: 交易方向
        """
        ZMBase.__init__(self)
        self._symbol = symbol
        self._logger = zm_log.get_log(f'volume_monitor_{self._symbol}')
        self._public_rest_api = None
        self._volume_rate = volume_rate
        self._trade_side = trade_side
        self._base_vol = 0

    def init_params(self):
        try:
            self._base_vol = BASE_VOLUME_DICT[self._symbol]
            self._public_rest_api = BinancePublicPerpApi(self._symbol, self._logger)
            return
        except Exception as e:
            error_info = "%s,%s" % (e, traceback.format_exc())
            self._logger.error(error_info)
            self.send_wechat(self._mail_to, "HFT量监控异常", error_info)
            raise

    def start(self):
        self.init_params()
        last_time = time.time()
        while True:
            try:
                last_time = self.pace_cycle(last_time, cyc_time=1)
                klines = self._public_rest_api.get_klines()
                vol0 = float(klines[0][5])
                if vol0 < self._base_vol * 0.2:
                    self._trade_side.value = -1
                else:
                    self._trade_side.value = float(klines[0][9]) / vol0   # 主动买入占比
                vol1 = float(klines[1][5])
                volume = max(vol0, vol1)
                self._volume_rate.value = volume / self._base_vol
                if self._volume_rate.value < 0.8:
                    time.sleep(3)
                # self._logger.info(f"volume_rate:{self._volume_rate.value}, trade_side:{self._trade_side.value}")
            except (socket.timeout, http.client.RemoteDisconnected, http.client.CannotSendRequest)  as e:
                err_msg = repr(e)
                self._logger.error(err_msg)
                self.send_wechat(self._mail_to, "VolumeMonitor Exception1", err_msg)
                self._public_rest_api.init_https_connection()
            except Exception as e:
                error_info = "%s,%s" % (e, traceback.format_exc())
                self._logger.error(error_info)
                self.send_wechat(self._mail_to, 'VolumeMonitor Exception2', error_info)
                # 重建http连接
                self._public_rest_api.init_https_connection()
                time.sleep(10)


if __name__ == '__main__':
    RunEnv.set_run_env('test')
    _symbol = "doge_usdt"
    b = VolumeMonitor(_symbol, None, None)
    b.start()
