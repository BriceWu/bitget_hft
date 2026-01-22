#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
import traceback, socket, http.client, time, os, sys
from multiprocessing import Process, Value
from zmqfsi.service.zm_base import ZMBase
from zmqfsi.util.zm_env import RunEnv
import zmqfsi.util.zm_log as zm_log
from volume_monitor import VolumeMonitor

def __volume_monitor(env, symbol, volume_rate, trade_side):
    RunEnv.set_run_env(env)
    vm = VolumeMonitor(symbol, volume_rate, trade_side)
    vm.start()


class HFTStrategy(ZMBase):
    def __init__(self, symbol, mark):
        """
        初始化
        :param symbol: 交易对
        """
        ZMBase.__init__(self)
        self._symbol = symbol
        self._logger = zm_log.get_log(f'{os.path.basename(sys.argv[0])[:-3]}_{self._symbol}')
        self._public_rest_api = None
        self._volume_rate = volume_rate
        self._trade_side = trade_side
        self._base_vol = 0

    def init_params(self):
        try:
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
                pass
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
    _env = RunEnv.get_run_env()
    _symbol = "doge_usdt"
    v_trade_side = Value('i', 0)
    v_volume_rate = Value('d', 0)
    Process(target=__volume_monitor, args=(_env, _symbol, v_trade_side, v_volume_rate)).start()
    while True:
        time.sleep(1)
        print("开始打印......")
        print(v_trade_side.value)
        print(v_volume_rate.value)
