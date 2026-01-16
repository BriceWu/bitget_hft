#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
import time
import orjson, json
import base64
import hmac, hashlib, http.client
from zmqfsi.util.zm_client import ZMClient
from bit1024.account.base import AccountBase
from config import HFT_KEYS_COLLECTION


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
        self._https_client = None
        self._get_header = None
        self._post_header = None

    def set_key_info(self):
        _access_key, _secret_key, _pass_phrase = self._get_key_info(keys_collection=HFT_KEYS_COLLECTION, strategy_name="HFT")
        self._secret_key = _secret_key.encode()
        self._get_header = {
            "ACCESS-KEY": _access_key,
            "ACCESS-PASSPHRASE": _pass_phrase
        }
        self._post_header = {
            "ACCESS-KEY": _access_key,
            "ACCESS-PASSPHRASE": _pass_phrase,
            "Content-Type": "application/json"
        }

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

    def get_position_info(self):
        path = f"/api/v2/mix/position/single-position?productType=USDT-FUTURES&symbol={self._format_symbol}"
        return self.http_get(path)

    def http_get(self, path):
        timestamp = str(int(time.time() * 1000))
        self._get_header['ACCESS-SIGN'] = base64.b64encode(hmac.new(self._secret_key, (timestamp + "GET" + path).encode(),digestmod=hashlib.sha256).digest())
        self._get_header['ACCESS-TIMESTAMP'] = timestamp
        self._https_client.request(method="GET", url=path, headers=self._get_header)
        response = self._https_client.getresponse()
        body = response.read()
        json_data = orjson.loads(body)
        return json_data
