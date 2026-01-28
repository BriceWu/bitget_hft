#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
import sys, os, traceback, time
from sys import argv
import zmqfsi.util.zm_log as zm_log
from zmqfsi.service.zm_base import ZMBase
from zmqfsi.util.zm_env import RunEnv
from api.bitget_perp_api import BitgetPerpApi
import zmqfsi.util.zm_date as zm_date

REBATE = 0.5

class ProfitStatistics(ZMBase):
    def __init__(self, symbol, mark):
        ZMBase.__init__(self)
        self._symbol = symbol
        self._mark = mark
        self._logger = zm_log.get_log(f"{os.path.basename(sys.argv[0])[:-3]}_{self._symbol}_{self._mark}_bitget")
        self._rest_api = None

    def start(self):
        self.do_statistics_operation()

    def get_start_end_time(self):
        time_now = int(time.time())
        time_str = zm_date.get_local_time(time_now)
        end_time = time_str[:-5] + "00:00"
        last_time_str = zm_date.get_local_time((time_now - 60*60))  # 往前推一个小时
        start_time = last_time_str[:-5] + "00:00"
        self._logger.info(f"start_time:{start_time}, end_time:{end_time}")
        return zm_date.get_time_stamp(start_time) * 1000, zm_date.get_time_stamp(end_time) * 1000 - 1

    def statistics_trade_orders_history(self):
        start_time, end_time = self.get_start_end_time()
        self._logger.info(f"end_time:{end_time}, start_time:{start_time}")
        real_trades_count = 0
        real_trades_amount = 0.
        trading_fee = 0.
        real_profit = 0.

        row_len = 100
        end_id = None
        while row_len == 100:
            time.sleep(1)
            trades = self._rest_api.get_trade_orders_history(end_time=end_time, start_time=start_time, end_id=end_id)
            if trades is None:
                time.sleep(3)
                trades = self._rest_api.get_trade_orders_history(end_time=end_time, start_time=start_time, end_id=end_id)
            if trades is None or trades['code'] != '00000':
                self._logger.error(trades)
                self.send_wechat(self._mail_to, "交易历史获取失败", trades)
                time.sleep(15)
                raise Exception("交易历史获取失败")
            rows = trades['data']['fillList']
            row_len = len(rows)
            self._logger.info(f"交易历史 start_time:{start_time}, end_time:{end_time}, row_len:{row_len}")
            for r in rows:
                if r['quoteVolume'] == '0':
                    continue
                real_trades_count += 1
                real_trades_amount += float(r['quoteVolume'])
                fee_list = r['feeDetail']
                for fee_item in fee_list:
                    trading_fee += float(fee_item['totalFee'])
                real_profit += float(r['profit'])
                enter_point_source = r['enterPointSource']
                if enter_point_source not in ["web", "sys"]:
                    self._logger.error(json.dumps(r))
                    self.send_wechat(self._mail_to,"异常来源订单", r)
                    time.sleep(3)
            end_id = trades['data']['endId']
        self._logger.error(f"查询,真实成交次数：{real_trades_count}, 真实成交金额：{real_trades_amount}, 交易手续费：{trading_fee}, 实际盈亏：{real_profit}")
        self.save_statistics_result(start_time, end_time, real_trades_count, real_trades_amount, trading_fee, real_profit)

    def do_statistics_operation(self):
        statistics_sleep_time = 20*60
        last_time = 0
        while True:
            try:
                last_time = self.process_sleep(last_time, cyc_time=statistics_sleep_time)
                self._rest_api = BitgetPerpApi(self._symbol, self._mark, self._logger)
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

