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
        self._secret_key = None
        self._format_symbol = self.format_symbol()
