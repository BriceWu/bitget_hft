# !/usr/bin/python
# -*- coding: utf-8 -*-
import json
import sys
import time
from api.ws_socket_base import WSSocketBase


class BitgetPerpWSApiAsync(WSSocketBase):
    def __init__(self, symbol):
        WSSocketBase.__init__(self)
        self._symbol = symbol
        self._format_symbol = None
        self._tgt_platform = "bitget_perp"
        self.format_symbol(self._symbol)
        self._ws_url = "wss://ws.bitget.com/v2/ws/public"
        self.last_ping_time = sys.maxsize

    def format_symbol(self, symbol):
        self._format_symbol = symbol.replace('_', '').upper()

# region 订阅深度
    async def subscribe_data(self):
        subscribe_message = {
            "op": "subscribe",
            "args": [
                {
                    "instType": "USDT-FUTURES",
                    "channel": "books1",
                    "instId": self._format_symbol
                }
            ]
        }
        sub_mes = json.dumps(subscribe_message)
        await self.ws_client.send(sub_mes)
        self.last_ping_time = time.time()

# endregion


