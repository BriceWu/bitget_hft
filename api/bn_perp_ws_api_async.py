# !/usr/bin/python
# -*- coding: utf-8 -*-
import asyncio
import time
import traceback
import json, orjson
import aiohttp
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
        self.interval = 2
        self._rest_timeout = aiohttp.ClientTimeout(2)

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

    async def query_depth_rest(self):
        url = "https://fapi.binance.com/fapi/v1/depth"
        params = {
            'symbol': self._format_symbol.upper(),
            'limit': 5
        }
        headers = {}
        try:
            async with aiohttp.ClientSession(timeout=self._rest_timeout) as session:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        return None
        except asyncio.TimeoutError as e:
            message = f"{self._symbol} TimeoutError;url:{url};exception:{e}"
            self._logger.info(message)
            return None
        except Exception as e:
            self._logger.error(e)
            await asyncio.sleep(2)
            return None

    async def analysis_rest_depth(self):
        """
        转化成和ws格式一样的
        :return:
        """
        result = await self.query_depth_rest()
        if result is None:
            return None
        else:
            return result

    async def analysis(self, exec_ws_strategy, exec_rest_strategy):
        self._logger.info("Start Analysis ......")
        last_update_id = self.update_id
        last_req_time = time.time()
        last_time = 0
        while True:
            try:
                last_time = await self.async_process_sleep(last_time, cyc_time=50)
                if last_update_id == self.update_id:
                    cur_time = time.time()
                    if cur_time - last_req_time > self.interval:
                        last_req_time = cur_time
                        data = await self.analysis_rest_depth()
                        if data:
                            exec_rest_strategy(data)
                    else:
                        await asyncio.sleep(0.003)
                    continue
                last_update_id = self.update_id
                last_req_time = self.update_id
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
