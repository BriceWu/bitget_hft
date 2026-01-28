#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
import sys, os, traceback, time, json
from sys import argv
import zmqfsi.util.zm_log as zm_log
from zmqfsi.service.zm_base import ZMBase
from zmqfsi.util.zm_env import RunEnv
from api.bitget_perp_api import BitgetPerpApi
from datetime import datetime

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

    def statistics_profit_data(self, msg_dic):
        time_now = int(time.time() * 1000)
        rows_item = self.query_cash_flow()
        trading_fee = 0.
        funding_fee = 0.
        trading_profit_long = 0.
        trading_profit_short = 0.
        trading_profit_liquidation = 0.

        trading_fee_2d = 0.
        funding_fee_2d = 0.
        trading_profit_long_2d = 0.
        trading_profit_short_2d = 0.
        trading_profit_liquidation_2d = 0.

        start_2d = time_now - 2 * 24 * 60 * 60 * 1000
        for row in rows_item:
            income_type = row['businessType']  # amount = "0"是开仓
            if income_type in ['sell', 'buy', 'open_long', 'close_long', 'open_short', 'close_short']:  # 交易
                trading_fee += float(row['fee'])
                amount = float(row['amount'])
                if amount != 0.:
                    if income_type in ['open_long', 'open_short']:
                        error_msg = f"异常成交信息：{json.dumps(row)}"
                        self._logger.error(error_msg)
                        self.send_wechat(self._mail_to, "异常成交信息", row)
                        raise Exception(error_msg)
                    elif income_type in ['close_long', 'sell']:
                        trading_profit_long += amount
                    elif income_type in ['close_short', 'buy']:
                        trading_profit_short += amount
            elif income_type == 'contract_settle_fee':
                funding_fee += float(row['amount'])
            elif income_type in ['burst_buy', 'burst_sell', 'burst_short_loss_query', 'burst_long_loss_query']:  # 爆仓
                trading_profit_liquidation += float(row['amount'])
                trading_fee += float(row['fee'])
            elif income_type == 'risk_captital_user_transfer':  # 爆仓清算
                trading_profit_liquidation += float(row['amount'])  # 清算是没有手续费的
            elif income_type in ['trans_from_exchange', 'trans_to_exchange', 'append_margin', 'adjust_down_lever_append_margin', 'trans_from_contract', 'trans_to_strategy', 'trans_from_strategy']:
                pass
            else:
                self._logger.error(f"异常资金流水：{json.dumps(row)}")
                self.send_wechat(self._mail_to, "异常资金流水", row)
                # raise Exception("异常资金流水")

            if int(row['cTime']) >= start_2d:
                trading_fee_2d = trading_fee
                funding_fee_2d = funding_fee
                trading_profit_liquidation_2d = trading_profit_liquidation
                trading_profit_long_2d = trading_profit_long
                trading_profit_short_2d = trading_profit_short
        msg_dic['LongProfit'] = round(trading_profit_long, 2)
        msg_dic['ShortProfit'] = round(trading_profit_short, 2)
        msg_dic['TradingFee'] = trading_fee
        msg_dic['TotalProfit_7d'] = trading_profit_long + trading_profit_short + trading_fee * (1-REBATE) + funding_fee + trading_profit_liquidation
        if funding_fee != 0.:
            msg_dic['FundingFee'] = round(funding_fee, 2)
        if trading_profit_liquidation != 0.:
            msg_dic['LiquidationProfit'] = round(trading_profit_liquidation, 2)

        msg_dic['LongProfit_2d'] = round(trading_profit_long_2d, 2)
        msg_dic['ShortProfit_2d'] = round(trading_profit_short_2d, 2)
        msg_dic['TradingFee_2d'] = round(trading_fee_2d, 2)
        msg_dic['TotalProfit_2d'] = round(trading_profit_long_2d + trading_profit_short_2d + trading_fee_2d * (1-REBATE) + funding_fee_2d + trading_profit_liquidation_2d, 2)
        if funding_fee_2d != 0.:
            msg_dic['FundingFee_2d'] = round(funding_fee_2d, 2)
        if trading_profit_liquidation_2d != 0.:
            msg_dic['LiquidationProfit_2d'] = round(trading_profit_liquidation_2d, 2)

    def query_cash_flow(self):
        end_time = int(time.time() * 1000)
        start_time = max(end_time - 7 * 24 * 60 * 60 * 1000, 1749798000000)  # 过去7天，纳秒
        self._logger.info(f"end_time:{end_time}, start_time:{start_time}")
        trade_rows = []

        row_len = 100
        end_id = None
        while row_len==100:
            time.sleep(1)
            trades = self._rest_api.get_cash_flow_without_symbol(end_time=end_time, start_time=start_time,repay_id=end_id)
            if trades is None:
                time.sleep(3)
                trades = self._rest_api.get_cash_flow_without_symbol(end_time=end_time, start_time=start_time, repay_id=end_id)
            if trades is None or trades['code'] != '00000':
                self._logger.error(trades)
                self.send_wechat(self._mail_to, "资金流水获取失败", trades)
                time.sleep(15)
                raise Exception("资金流水获取失败")
            data = trades['data']
            rows = data['bills']
            row_len = len(rows)
            self._logger.info(f"资金流水, row_len:{row_len}")
            trade_rows.extend(rows)
            end_id = data['endId']
        return trade_rows

    def do_statistics_operation(self):
        statistics_sleep_time = 30*60
        last_time = 0
        while True:
            try:
                last_time = self.pace_cycle(last_time, cyc_time=statistics_sleep_time)
                msg_dic = {}
                self._rest_api = BitgetPerpApi(self._symbol, self._mark, self._logger)
                self.statistics_profit_data(msg_dic)
                self._logger.info(json.dumps(msg_dic))
                self.send_wechat(self._mail_to, "ProfitStatistics", msg_dic)
                self.delete_history_log()
            except Exception as e:
                error_info = "%s,%s" % (e, traceback.format_exc())
                self._logger.info(error_info)
                self.send_wechat(self._mail_to, 'ProfitStatistics Exception', error_info)
                time.sleep(60)
                last_time = 0.

    def delete_history_log(self):
        time_now = datetime.now()
        if time_now.hour != 7:  # utc时间
            return
        if time_now.minute >= 20:  # 15点 ~ 15点20分 执行
            return
        directory = 'log'
        files = os.listdir(directory)
        for file in files:
            split_names = file.split(".")
            if split_names[-2] != 'log':
                continue
            file_path = os.path.join(directory, file)
            if os.path.isfile(file_path):
                self._logger.error(f"删除文件：{file_path}")
                os.remove(file_path)


if __name__ == "__main__":
    RunEnv.set_run_env(argv[1])
    _symbol = argv[2]
    _mark = "xyz369free"
    account = ProfitStatistics(_symbol, _mark)
    account.start()

