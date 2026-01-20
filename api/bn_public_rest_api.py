#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
import traceback
import orjson
from zmqfsi.service.zm_base import ZMBase
from zmqfsi.util.zm_env import RunEnv
import http.client
import zmqfsi.util.zm_log as zm_log



class BinancePublicPerpApi(ZMBase):
    def __init__(self, symbol, logger):
        ZMBase.__init__(self)
        self._symbol = symbol
        self._logger = logger
        if logger is None:
            raise Exception("日志对象未初始化")
        self._host_address = "fapi.binance.com"
        self._format_symbol = self.format_symbol()
        self._https_client = None
        self.init_https_connection()

# region 初始化

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

    def init_https_connection(self):
        """
        初始化https连接 [仅仅支持https]
        :return:
        """
        try:
            if self._https_client:
                self._https_client.close()
            if self._run_env != "test":
                https_conn = http.client.HTTPSConnection(self._host_address, timeout=10)
            else:
                https_conn = http.client.HTTPSConnection("127.0.0.1", port=10809, timeout=10)
                https_conn.set_tunnel(self._host_address)
            self._https_client = https_conn
        except Exception as e:
            error_info = "%s,%s" % (e, traceback.format_exc())
            self._logger.error(error_info)
            self.send_wechat(self._mail_to, "HTTP连接初始化异常", error_info)
            raise

# endregion
    def get_klines(self):
        """
        获取K线, 包含当前这一分钟
        :return:
        """
        return self.http_get(f"/fapi/v1/klines?symbol={self._format_symbol}&interval=1m&limit=2")

# http 请求
    def http_get(self, path):
        self._https_client.request(method="GET", url=path)
        response = self._https_client.getresponse()
        body = response.read()
        json_data = orjson.loads(body)
        return json_data
# endregion

if __name__ == '__main__':
    RunEnv.set_run_env('test')
    _symbol = "doge_usdt"
    _logger = zm_log.get_log(_symbol)
    b = BinancePublicPerpApi(_symbol, _logger)
    b.get_klines()
