# !/usr/bin/python
# -*- coding: utf-8 -*-
import asyncio
import time
import traceback
import json, orjson
from api.ws_socket_base import WSSocketBase
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError


class BinancePerpWSApiAsync(WSSocketBase):
    def __init__(self, symbol):
        WSSocketBase.__init__(self)
        self._symbol = symbol
        self._format_symbol = None
        self._tgt_platform = "binance_perp"
        self.format_symbol(self._symbol)
        self._ws_url = "wss://fstream.binance.com/stream"

    def format_symbol(self, symbol):
        self._format_symbol = symbol.replace('_', '')

# region 订阅深度
    async def subscribe_ticker(self):
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": [f"{self._format_symbol}@bookTicker"],
            "id": 1
        }
        sub_mes = json.dumps(subscribe_message)
        await self.ws_client.send(sub_mes)

    async def start_ws(self):
        while True:
            try:
                await self.init_connection(time_out=1)
                await self.subscribe_ticker()
                await self.receive()
            except (ConnectionClosedOK, ConnectionClosedError) as e:
                self._logger.error(e)
                await asyncio.sleep(0.1)
            except Exception as e:
                error_info = "Start ws Exception: %s,%s" % (e, traceback.format_exc())
                self._logger.info(error_info)
                self.send_wechat(self._mail_to, "Connect Exception", f"{self._tgt_platform}{self._symbol}：{error_info}")
                await asyncio.sleep(1)
# endregion

    async def analysis(self, exec_ws_strategy):
        self._logger.info("Start Analysis ......")
        last_update_id = self.update_id
        last_time = 0
        while True:
            try:
                last_time = await self.async_process_sleep(last_time, cyc_time=5)
                if last_update_id == self.update_id:
                    continue
                last_update_id = self.update_id
                data = orjson.loads(self.ws_message)
                exec_ws_strategy(data)
            except Exception as e:
                error_info = "Analysis Exception: %s,%s" % (e, traceback.format_exc())
                self._logger.info(error_info)
                await asyncio.sleep(2)

    @staticmethod
    async def async_process_sleep(last_time, cyc_time=250):
        """
        异步进程休眠
        :param last_time: 上一轮标记时间(单位：ms)
        :param cyc_time: 循环时长
        :return: 本轮标记时间(单位：ms)
        """
        now_time = time.time() * 1000
        delta_time = now_time - last_time
        if delta_time < cyc_time:  # 计算时长小于cyc_time， 则休眠
            sleep_time = (cyc_time - delta_time) * 0.001
            await asyncio.sleep(sleep_time)
        return time.time() * 1000
