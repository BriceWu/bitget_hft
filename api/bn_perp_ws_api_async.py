# !/usr/bin/python
# -*- coding: utf-8 -*-
import asyncio
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

# endregion

    async def analysis(self, exec_ws_strategy):
        self._logger.info(f"Start Analysis ......{self._tgt_platform}")
        last_update_id = self.update_id
        last_time = 0
        while True:
            try:
                last_time = await self.pace_cycle_async(last_time, cyc_time=0.005)  # 5ms
                if last_update_id == self.update_id:
                    continue
                last_update_id = self.update_id
                data = orjson.loads(self.ws_message)
                exec_ws_strategy(data)
            except Exception as e:
                error_info = "Analysis Exception[{%s}]: %s,%s" % (self._tgt_platform, e, traceback.format_exc())
                self._logger.info(error_info)
                await asyncio.sleep(2)


