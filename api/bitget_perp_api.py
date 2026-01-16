#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
import time
import orjson, json
import base64
import hmac, hashlib, http.client
from zmqfsi.util.zm_client import ZMClient
from bit1024.account.base import AccountBase


class BitgetPerpApi(AccountBase):
    def __init__(self, symbol, logger):
        AccountBase.__init__(self)
        self._symbol = symbol
        self._logger = logger
        if logger is None:
            raise Exception("日志对象未初始化")
        self._host_address = "api.bitget.com"
        self._secret_key = None
        self._format_symbol = self.format_symbol()

    def format_symbol(self, symbol=None):
        """
        按照bitget的方式格式化交易对
        :param symbol:交易对
        :return:格式化后的交易对
        """
        if not symbol:
            return self._symbol.replace('_', "").upper()
        else:
            return symbol.replace('_', "").upper()
