#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
import traceback, socket, http.client, os, sys, asyncio, orjson
from zmqfsi.util.zm_client import ZMClient
from multiprocessing import Process, Value
from zmqfsi.service.zm_base import ZMBase
from zmqfsi.util.zm_env import RunEnv
import zmqfsi.util.zm_log as zm_log

from volume_monitor import VolumeMonitor
from api.bn_perp_ws_api_async import BinancePerpWSApiAsync
from api.bitget_perp_ws_api_async import BitgetPerpWSApiAsync
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
        self._bn_ws_api = None
        self._bitget_ws_api = None
        self._rest_api = None

        self._bn_ask_one = None
        self._bn_bid_one = None
        self._bitget_ask_one = None
        self._bitget_bid_one = None

    def init_params(self):
        try:
            ZMClient.set('logger', self._logger)
            self._rest_api = BitgetPerpApi(self._symbol, self._mark, self._logger)
            if self._run_env == 'test':
                import socks, socket
                socks.set_default_proxy(socks.HTTP, "127.0.0.1", 10809)  # 设置全局代理
                socket.socket = socks.socksocket
            self._bn_ws_api = BinancePerpWSApiAsync(self._symbol)
            self._bitget_ws_api = BitgetPerpWSApiAsync(self._symbol)
            return
        except Exception as e:
            error_info = "%s,%s" % (e, traceback.format_exc())
            self._logger.error(error_info)
            self.send_wechat(self._mail_to, "HFT策略异常", error_info)
            raise

    async def run_tasks(self):
        self.init_params()
        self.send_wechat(self._mail_to, "HFT Start", "HFT Start")
        self._logger.info("HFT start")
        # 同时运行 task_1 和 task_2
        await asyncio.gather(self._bn_ws_api.start_ws(), self._bitget_ws_api.start_ws(), self.start_hft())  # 创建任务

    async def start_hft(self):
        last_time = 0
        last_bn_update_id = -1
        last_bitget_update_id = -1
        while True:
            try:
                last_time = await self.pace_cycle_async(last_time, cyc_time=0.004)  # 4ms
                if (last_bitget_update_id == self._bitget_ws_api.update_id) and (last_bn_update_id == self._bn_ws_api.update_id):
                    continue
                self._logger.info(self._bn_ws_api.ws_message)
                self._logger.info(self._bitget_ws_api.ws_message)
                last_bitget_update_id = self._bitget_ws_api.update_id
                last_bn_update_id = self._bn_ws_api.update_id
            except (socket.timeout, http.client.RemoteDisconnected, http.client.CannotSendRequest)  as e:
                err_msg = repr(e)
                self._logger.error(err_msg)
                self.send_wechat(self._mail_to, "HFT Exception1", err_msg)
                self._rest_api.init_https_connection()
            except Exception as e:
                error_info = "%s,%s" % (e, traceback.format_exc())
                self._logger.error(error_info)
                self.send_wechat(self._mail_to, 'HFT Exception2', error_info)
                # 重建http连接
                self._rest_api.init_https_connection()
                await asyncio.sleep(5)

    def analysis_bn_bs_one(self):
        val = orjson.loads(self._bn_ws_api.ws_message)
        data = val['data']
        self._bn_ask_one = data['a']
        self._bn_bid_one = data['b']

    def analysis_bitget_ws_one(self):
        pass


if __name__ == '__main__':
    RunEnv.set_run_env('test')
    _env = RunEnv.get_run_env()
    _symbol = "doge_usdt"
    _mark = "xyz369free"
    v_trade_side = Value('d', 0)
    v_volume_rate = Value('d', 0)
    Process(target=__volume_monitor, args=(_env, _symbol, v_volume_rate, v_trade_side)).start()
    loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(loop)
    active = HFTStrategy(_symbol, _mark, v_volume_rate, v_trade_side)
    loop.run_until_complete(active.run_tasks())
