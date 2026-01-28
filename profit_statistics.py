#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
import sys, os, traceback, time
from sys import argv
import zmqfsi.util.zm_log as zm_log
from zmqfsi.service.zm_base import ZMBase
from zmqfsi.util.zm_env import RunEnv
sys.path.append(sys.path[0])

REBATE = 0.5

class ProfitStatistics(ZMBase):
    def __init__(self, symbol, mark):
        ZMBase.__init__(self)
        self._symbol = symbol
        self._mark = mark
        self._logger = zm_log.get_log(f"{os.path.basename(sys.argv[0])[:-3]}_{self._symbol}_{self._mark}_bitget")
        self._this_rest_api = None

    def start(self):
        self.init_params()
        self.do_statistics_operation()

    def do_statistics_operation(self):
        statistics_sleep_time = 20*60
        last_time = 0
        while True:
            try:
                last_time = self.process_sleep(last_time, cyc_time=statistics_sleep_time)
                self.statistics_account_profit()
            except Exception as e:
                error_info = "%s,%s" % (e, traceback.format_exc())
                self._logger.info(error_info)
                self.send_wechat(self._mail_to, f'Account Equity Exception[{self._server}]', error_info)
                self._this_rest_api.init_http_connection()
                time.sleep(15)


if __name__ == "__main__":
    RunEnv.set_run_env(argv[1])
    _symbol = argv[2]
    _mark = "xyz369free"
    account = ProfitStatistics(_symbol, _mark)
    account.start()

