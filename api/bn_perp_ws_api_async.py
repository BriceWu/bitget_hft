# !/usr/bin/python
# -*- coding: utf-8 -*-
import asyncio
import traceback
import json
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


