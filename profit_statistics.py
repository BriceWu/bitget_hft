#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
import json
import sys, os, traceback, time
from sys import argv
from datetime import datetime
import zmqfsi.util.zm_log as zm_log
from zmqfsi.service.zm_base import ZMBase
from zmqfsi.util.zm_client import ZMClient
from zmqfsi.util.zm_env import RunEnv
import zmqfsi.util.zm_date as zm_date
sys.path.append(sys.path[0])
from api.bitget_perp_api import BitgetPerpApi

REBATE = 0.6
DEFAULT_TIME_VAL = '-1'

class BitgetAccountStatistics(ZMBase):
    def __init__(self, display_symbol):
        ZMBase.__init__(self)
        self._display_symbol = display_symbol
        self._this_platform = "bitget_perp"
        self._logger = zm_log.get_log(f"{os.path.basename(sys.argv[0])[:-3]}_{self._display_symbol}_bitget")
        self._this_rest_api = None
        self._position_rest_api = None
        self._statistics_rest_api = None
        self._api_user_id = None
        self._position_key = None
        self._server = None
        self._biz_mongo = None
        self._trade_redis = None
        self._asset_key = None
        self._small_cycle_time = 4 * 1000

        self._position_cyc_time = POSITION_TIME_OUT_MS * 0.4

    def start(self):
        self.init_params()
        threading.Thread(target=self.set_inside_position).start()
        self.do_statistics_operation()

    def do_statistics_operation(self):
        statistics_sleep_time = 10*60
        last_statistics_time = time.time() - statistics_sleep_time
        last_time = 0
        try:
            symbol = "xrp_usdt"
            # _this_web_api = BitgetWebApi(self._display_symbol, symbol, self._logger)
            # _this_web_api.init_backup_socket()
            # _this_web_api.set_open_hedge_mode_order_str(3, 3, f"{symbol.replace('_','')}{int(time.time()*1000)}")
            # self._logger.error(_this_web_api._open_hedge_mode_short_str)
            # body = _this_web_api.open_hedge_mode_short_order(p_price=2.3, tp_price=2.2, tp_trigger=2.25, st_trigger=2.4)
            # self._logger.error(f"web开空：{json.dumps(body)}")
        except Exception as e:
            error_info = "%s,%s" % (e, traceback.format_exc())
            self._logger.info(error_info)
        while True:
            try:
                last_time = self.process_sleep(last_time, cyc_time=self._small_cycle_time)
                account_info, asset_free = self.analysis_account_info()
                if account_info is None:
                    time.sleep(2 * 60)
                    self._this_rest_api.init_http_connection()
                    continue
                if time.time() - last_statistics_time >= statistics_sleep_time:
                    last_statistics_time = time.time()
                    threading.Thread(target=self.statistics_account_profit).start()
            except (socket.timeout, http.client.RemoteDisconnected, http.client.CannotSendRequest, ConnectionResetError)  as e:
                err_msg = repr(e)
                self._logger.error(err_msg)
                self.send_wechat(self._mail_to, f"Account Equity Exception[{self._server}]", err_msg)
                self._this_rest_api.init_http_connection()
                time.sleep(5)
            except Exception as e:
                error_info = "%s,%s" % (e, traceback.format_exc())
                self._logger.info(error_info)
                self.send_wechat(self._mail_to, f'Account Equity Exception[{self._server}]', error_info)
                self._this_rest_api.init_http_connection()
                time.sleep(15)

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
            trades = self._statistics_rest_api.get_cash_flow_without_symbol(end_time=end_time, start_time=start_time,repay_id=end_id)
            if trades is None:
                time.sleep(3)
                trades = self._statistics_rest_api.get_cash_flow_without_symbol(end_time=end_time, start_time=start_time, repay_id=end_id)
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
            trades = self._statistics_rest_api.get_trade_orders_history(end_time=end_time, start_time=start_time, end_id=end_id)
            if trades is None:
                time.sleep(3)
                trades = self._statistics_rest_api.get_trade_orders_history(end_time=end_time, start_time=start_time, end_id=end_id)
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

    def get_trade_amount(self):
        t_start, _ = self.get_start_end_time()
        start_time = t_start - 2 * 24 * 60 * 60 * 1000
        self._logger.info(f'start_time:{start_time}')
        _filters = {"_id": {"$regex": f"^{self._this_platform}_{self._api_user_id}_statistics_.*"}, "start": {"$gte": start_time}}
        trade_orders = self._biz_mongo.find_many("ArbitrageBotData", filter=_filters)
        amount = 0.
        for t in trade_orders:
            amount += t['amount']
        return int(amount)

    def save_statistics_result(self, start_time, end_time, trades_count, trades_amount, trading_fee, profit):
        msg = {
            "_id": f"{self._this_platform}_{self._api_user_id}_statistics_{start_time}",
            "start": start_time,
            "end": end_time,
            "count": trades_count,
            "amount": trades_amount,
            "fee": trading_fee,
            "profit": profit
        }
        self._biz_mongo.save_record(msg, "ArbitrageBotData")

    def get_start_end_time(self):
        time_now = int(time.time())
        time_str = zm_date.get_local_time(time_now)
        end_time = time_str[:-5] + "00:00"
        last_time_str = zm_date.get_local_time((time_now - 60*60))  # 往前推一个小时
        start_time = last_time_str[:-5] + "00:00"
        self._logger.info(f"start_time:{start_time}, end_time:{end_time}")
        return zm_date.get_time_stamp(start_time) * 1000, zm_date.get_time_stamp(end_time) * 1000 - 1

    def get_equity(self):
        asset_str = self._trade_redis.get(self._asset_key)
        asset = json.loads(asset_str)
        msg_dic = {"Equity": asset['equity']}
        if asset['frozen'] != 0.:
            msg_dic['Frozen'] = asset['frozen']
        return msg_dic

    def get_spot_account_assets(self):
        """
        获取现货资产
        :return:
        """
        spot_asset = self._statistics_rest_api.get_spot_assets()
        data = spot_asset['data']
        asset_dic = {}
        for item in data:
            coin = item['coin']
            val = float(item["available"]) + float(item["frozen"])
            asset_dic[coin] = val
        # if asset_dic:
        #     self.send_wechat(self._mail_to, "Spot Asset", asset_dic)
        return asset_dic

    def init_params(self):
        try:
            self._biz_mongo = ZMClient.get('biz_mongo')
            self._this_rest_api = BitgetPerpApi(self._display_symbol, "btc_usdt", self._logger)
            self._position_rest_api = BitgetPerpApi(self._display_symbol, "btc_usdt", self._logger, t_out=1)
            self._statistics_rest_api = BitgetPerpApi(self._display_symbol, "btc_usdt", self._logger, t_out=6)
            self._api_user_id = self._this_rest_api.get_user_id()
            self._server = self._this_rest_api.get_server()
            self._trade_redis = ZMClient.get("trade_redis")
            self._asset_key = f"{self._this_platform}_bot_{self._api_user_id}_assets"
            self._position_key = f"{self._this_platform}_bot_{self._api_user_id}_inside_positions"
        except Exception as e:
            error_info = "%s,%s" % (e, traceback.format_exc())
            self._logger.info(error_info)
            self.send_wechat(self._mail_to, f'Init Exception[{self._server}]', error_info)
            time.sleep(15)

    def analysis_account_info(self):
        account_info = self._this_rest_api.get_account_info()
        if not account_info:
            time.sleep(1)
            account_info = self._this_rest_api.get_account_info()
        if (not account_info) or (account_info.get("code") != '00000'):
            msg = "账户资金获取失败" if (not account_info) else json.dumps(account_info)
            self._logger.error(msg)
            self.send_wechat(self._mail_to, "账户资金获取失败", msg)
            return None, None
        asset_free = self.set_usdt_equity(account_info)
        return account_info, asset_free

    def set_usdt_equity(self, account_info):
        # self._logger.info(json.dumps(account_info))
        balance = account_info['data'][0]
        equity = float(balance['accountEquity'])
        usdt_equity = float(balance['usdtEquity'])
        if equity and abs(usdt_equity / equity -1) > 0.00001:  # 十万分之一
            msg = f"账户权益与USDT权益误差较大：" + json.dumps(account_info)
            self._logger.error(msg)
            self.send_wechat(self._mail_to, "账户权益与USDT权益误差较大", account_info)
            raise Exception(msg)
        free = float(balance['available'])
        frozen = float(balance['locked']) + float(balance['unrealizedPL']) + float(balance['isolatedMargin']) + float(balance['crossedMargin'])
        assets_dic = {"free": free, "frozen": frozen, "equity": equity}
        self._trade_redis.set(self._asset_key, json.dumps(assets_dic), ex=ACCOUNT_REDIS_EXPIRE)
        return free

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

# region 仓位

    def api_get_all_inside_position(self):
        for _ in range(5):
            posi = self._position_rest_api.get_position_info_without_symbol()
            if posi:
                err_code = posi['code']
                if err_code == '00000':
                    return posi
                self._logger.error(json.dumps(posi))
                self.send_wechat(self._mail_to, f"仓位接口异常{self._api_user_id}", posi)
                if err_code == '429':  # 限流了
                    time.sleep(random.uniform(10, 30))
                time.sleep(random.uniform(1, 2))
            else:
                time.sleep(self._position_cyc_time * 0.5 * 0.001)
        msg = "站内仓位获取失败"
        self._logger.error(msg)
        self.send_wechat(self._mail_to, msg, msg)
        raise Exception(msg)

    def set_inside_position(self):
        self._logger.info("开始获取站内仓位")
        last_time = 0
        while True:
            try:
                last_time = self.process_sleep(last_time, self._position_cyc_time)
                open_close_dict = self.analysis_open_close_time()
                posi_data = self.api_get_all_inside_position()
                # self._logger.info(json.dumps(posi_data))
                posi_dic = {}
                posi_list = posi_data['data']
                for posi in posi_list:
                    tgt_symbol = posi['symbol']
                    if not tgt_symbol.endswith('USDT'):
                        error_msg = f"异常的仓位：{json.dumps(posi)}"
                        self._logger.error(error_msg)
                        raise Exception(error_msg)
                    symbol = tgt_symbol[:-4].lower() + "_usdt"
                    trade_side = posi['holdSide']
                    if symbol not in open_close_dict:
                        open_close_dict[symbol] = {
                            THIS_POSITION_LONG: {'open_time': DEFAULT_TIME_VAL, 'close_time': DEFAULT_TIME_VAL},
                            THIS_POSITION_SHORT: {'open_time': DEFAULT_TIME_VAL, 'close_time': DEFAULT_TIME_VAL}
                        }

                    open_close_time_dic = open_close_dict[symbol][trade_side]
                    open_time = int(open_close_time_dic['open_time'])
                    close_time = int(open_close_time_dic['close_time'])

                    posi_val_dic = {'vol': posi['total'], 'locked': posi['locked'], 'price': posi['openPriceAvg'], 'time': int(posi['uTime']), 't_open': open_time, 't_close': close_time}
                    if symbol not in posi_dic:
                        posi_dic[symbol] = {trade_side: posi_val_dic}
                    else:
                        posi_dic[symbol][trade_side] = posi_val_dic

                self._trade_redis.set(self._position_key, json.dumps(posi_dic), px=POSITION_TIME_OUT_MS)
            except (socket.timeout, http.client.RemoteDisconnected, http.client.CannotSendRequest, ConnectionResetError) as e:
                err_msg = repr(e)
                self._logger.error(err_msg)
                self.send_wechat(self._mail_to, f"Inside Position Exception[{self._server}]", err_msg)
                self._position_rest_api.init_http_connection()
            except Exception as e:
                error_info = "%s,%s" % (e, traceback.format_exc())
                self._logger.info(error_info)
                self.send_wechat(self._mail_to, f'Inside Position Exception[{self._server}]', error_info)
                self._position_rest_api.init_http_connection()
                time.sleep(15)

    def analysis_open_close_time(self):
        now_time = int(time.time() * 1000)
        start_time = now_time - 12 * 60 * 1000
        end_time = now_time + 1 * 60 * 1000
        result = self._position_rest_api.get_trade_orders_history(end_time, start_time)
        if result['code'] != '00000':
            self._logger.error(json.dumps(result))
            return {}
        data_list = result['data']['fillList']
        open_close_dict = {}
        for data in data_list:
            symbol = data['symbol'][:-4].lower() + "_usdt"
            order_side = data['side']
            trade_side = data['tradeSide']
            if symbol not in open_close_dict:
                open_close_dict[symbol] = {
                    THIS_POSITION_LONG: {'open_time': DEFAULT_TIME_VAL, 'close_time': DEFAULT_TIME_VAL},
                    THIS_POSITION_SHORT: {'open_time': DEFAULT_TIME_VAL, 'close_time': DEFAULT_TIME_VAL}
                }

            if trade_side == 'open':
                if order_side == 'buy':  # 开多
                    old_time = open_close_dict[symbol][THIS_POSITION_LONG]['open_time']
                    if old_time == DEFAULT_TIME_VAL:
                        open_close_dict[symbol][THIS_POSITION_LONG]['open_time'] = data['cTime']
                else:  # 开空
                    old_time = open_close_dict[symbol][THIS_POSITION_SHORT]['open_time']
                    if old_time == DEFAULT_TIME_VAL:
                        open_close_dict[symbol][THIS_POSITION_SHORT]['open_time'] = data['cTime']
            else:  # close
                if order_side == 'buy':  # 平多
                    old_time = open_close_dict[symbol][THIS_POSITION_LONG]['close_time']
                    if old_time == DEFAULT_TIME_VAL:
                        open_close_dict[symbol][THIS_POSITION_LONG]['close_time'] = data['cTime']
                else:  # 平空
                    old_time = open_close_dict[symbol][THIS_POSITION_SHORT]['close_time']
                    if old_time == DEFAULT_TIME_VAL:
                        open_close_dict[symbol][THIS_POSITION_SHORT]['close_time'] = data['cTime']
        return open_close_dict
# endregion

    def statistics_account_profit(self):
        # self._statistics_rest_api.asset_transfer(qty=200, type="FUND_PFUTURES")
        try:
            msg_dic = self.get_equity()
            self._statistics_rest_api.init_http_connection()
            spot_asset = self.get_spot_account_assets()
            self.statistics_trade_orders_history()
            self.statistics_profit_data(msg_dic)
            msg_dic['TradeAmount_2d'] = self.get_trade_amount()
            msg_dic['Total_U'] = msg_dic['Equity'] + spot_asset.get('USDT', 0.)
            if spot_asset:
                msg_dic['SPOT'] = spot_asset
            self._logger.info(json.dumps(msg_dic))
            self.send_wechat(self._mail_to, f"ACCOUNT EQUITY[{self._api_user_id}]", msg_dic)
            self.delete_history_log()
        except (socket.timeout, http.client.RemoteDisconnected, http.client.CannotSendRequest, ConnectionResetError) as e:
            err_msg = repr(e)
            self._logger.error(err_msg)
            self.send_wechat(self._mail_to, f"Statistics Account Exception[{self._server}]", err_msg)
        except Exception as e:
            error_info = "%s,%s" % (e, traceback.format_exc())
            self._logger.info(error_info)
            self.send_wechat(self._mail_to, f'Statistics Account Exception[{self._server}]', error_info)


if __name__ == "__main__":
    RunEnv.set_run_env(argv[1])
    _display = f"{argv[2]}_usdt"
    account = BitgetAccountStatistics(_display)
    account.start()

