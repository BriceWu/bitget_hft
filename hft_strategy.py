#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
import traceback, socket, http.client, time, os, sys, asyncio
from multiprocessing import Process, Value
from zmqfsi.service.zm_base import ZMBase
from zmqfsi.util.zm_env import RunEnv
import zmqfsi.util.zm_log as zm_log
from volume_monitor import VolumeMonitor
from api.bn_perp_ws_api_async import BinancePerpWSApiAsync
from api.bitget_perp_api import BitgetPerpApi

def __volume_monitor(env, symbol, volume_rate, trade_side):
    RunEnv.set_run_env(env)
    vm = VolumeMonitor(symbol, volume_rate, trade_side)
    vm.start()


class HFTStrategy(ZMBase):
    def __init__(self, symbol, mark, volume_rate, trade_side):
        """
        初始化
        :param symbol: 交易对
        """
        ZMBase.__init__(self)
        self._symbol = symbol
        self._mark = mark
        self._logger = zm_log.get_log(f'{os.path.basename(sys.argv[0])[:-3]}_{self._symbol}_{self._mark}')
        self.v_volume_rate = volume_rate
        self.v_trade_side = trade_side
        self._tgt_ws_api = None
        self._rest_api = None

    def init_params(self):
        try:
            self._tgt_ws_api = BinancePerpWSApiAsync(self._symbol)
            self._rest_api = BitgetPerpApi(self._symbol, self._mark, self._logger)
            return
        except Exception as e:
            error_info = "%s,%s" % (e, traceback.format_exc())
            self._logger.error(error_info)
            self.send_wechat(self._mail_to, "HFT策略异常", error_info)
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
    _mark = "xyz369free"
    v_trade_side = Value('i', 0)
    v_volume_rate = Value('d', 0)
    Process(target=__volume_monitor, args=(_env, _symbol, v_volume_rate, v_trade_side)).start()
    loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(loop)
    active = HFTStrategy(_symbol, _mark, v_volume_rate, v_trade_side)
    loop.run_until_complete(active.run_tasks())
