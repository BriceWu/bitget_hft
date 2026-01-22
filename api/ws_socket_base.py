# !/usr/bin/python
# -*- coding: utf-8 -*-
import time, orjson, asyncio
import traceback
import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError
import ssl
from zmqfsi.util.zm_client import ZMClient
from zmqfsi.service.zm_base import ZMBase


class WSSocketBase(ZMBase):
    def __init__(self):
        ZMBase.__init__(self)
        self._symbol = None
        self._logger = ZMClient.get('logger')
        self._ws_url = None
        self.ws_client = None
        self.ws_message = None
        self.update_id = -1

    async def init_connection(self, time_out):
        self.close()
        ssl_context = ssl._create_unverified_context()
        # ssl_context = ssl.create_default_context()
        self.ws_client = await websockets.connect(self._ws_url, ssl=ssl_context, open_timeout=time_out)
        self._logger.info(f"[{self._symbol}]ws初始化")

    def close(self):
        if self.ws_client:
            self.ws_client.close()

    async def receive(self):
        self._logger.info('Start Receive ......')
        self._logger.info(f'订阅结果：{await self.ws_client.recv()}')
        while True:
            try:
                self.ws_message = await self.ws_client.recv()  # 使用异步接收
                self.update_id = time.time()
            except (ConnectionClosedOK, ConnectionClosedError) as e:
                self._logger.error(e)
                raise e
            except Exception as e:
                error_info = "Receive Exception: %s,%s" % (e, traceback.format_exc())
                self._logger.error(error_info)
                self.send_wechat(self._mail_to, "Receive Exception", f"{self._tgt_platform}{self._symbol}：{error_info}")
                raise e