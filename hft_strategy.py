#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
import time, json
import traceback, socket, http.client, os, sys, asyncio, orjson
from zmqfsi.util.zm_client import ZMClient
from zmqfsi.service.zm_base import ZMBase
import zmqfsi.util.zm_log as zm_log

from api.bn_perp_ws_api_async import BinancePerpWSApiAsync
from api.bitget_perp_ws_api_async import BitgetPerpWSApiAsync
from api.bitget_perp_api import BitgetPerpApi

ORDER_AMOUNT = 15


class HFTStrategy(ZMBase):
    def __init__(self, symbol, mark, volume_rate, trade_side):
        """
        初始化
        :param symbol: 交易对
        """
        ZMBase.__init__(self)
        self._symbol = symbol
        self._coin = None
        self._mark = mark
        self._logger = zm_log.get_log(f'hft_strategy_{self._symbol}_{self._mark}')
        self.v_volume_rate = volume_rate
        self.v_trade_side = trade_side
        self._bn_ws_api = None
        self._bitget_ws_api = None
        self._rest_api = None

        self._bn_price_changed = False
        self._bn_ask_one = None
        self._bn_bid_one = None
        self._bitget_ask_one = None
        self._bitget_bid_one = None

        self._sell_profit_rate = None  # bn_ask / bitget_bid
        self._buy_profit_rate = None  # bn_bid / bitget_ask
        self._bb_price_list = None  # [(bn_ask, bn_bid, bitget_ask, bitget_bid), (bn_ask, bn_bid, bitget_ask, bitget_bid) ......]
        self.sum_bn_sell = 0.
        self.sum_bn_buy = 0.
        self.sum_bitget_sell = 0.
        self.sum_bitget_buy = 0.
        self._last_price_list_update_time = 0
        self._price_rate_update_interval = 90
        self._bb_price_list_max_len = int(1 * 60 * 60 / self._price_rate_update_interval)

        self._order_vol = None
        self._client_open_order_id = None
        self._client_close_order_id = None

        self._pre_accuracy = 0
        self._post_accuracy = 5
        self._have_placed_order = 0.  # 下开仓单

        self._last_close_price = None
        self._open_position_price = None
        self._open_position_side = None

        self._close_position_delta_time = 4.5 * 60

    def init_params(self):
        try:
            self._coin = self._symbol.split("_")[0]
            self.sum_bn_sell = 0.
            self.sum_bn_buy = 0.
            self.sum_bitget_sell = 0.
            self.sum_bitget_buy = 0.
            self._bb_price_list = []
            ZMClient.set('logger', self._logger)
            self._rest_api = BitgetPerpApi(self._symbol, self._mark, self._logger)
            self.init_api_config(40)
            if self._run_env == 'test':
                import socks, socket
                socks.set_default_proxy(socks.HTTP, "127.0.0.1", 10809)  # 设置全局代理
                socket.socket = socks.socksocket
            self._bn_ws_api = BinancePerpWSApiAsync(self._symbol)
            self._bitget_ws_api = BitgetPerpWSApiAsync(self._symbol)
            return
        except Exception as e:
            error_info = "%s,%s" % (e, traceback.format_exc())
            self._logger.error(error_info)
            self.send_wechat(self._mail_to, "HFT策略异常", error_info)
            raise

    def init_api_config(self, leverage):
        result = self._rest_api.change_position_mode()  # 单向持仓
        self._logger.info(json.dumps(result))
        result2 = self._rest_api.change_margin_mode()  # 逐仓
        self._logger.info(json.dumps(result2))
        result3 = self._rest_api.adjust_leverage(leverage)
        self._logger.info(json.dumps(result3))

    async def run_tasks(self):
        self.init_params()
        self.send_wechat(self._mail_to, "HFT Start", "HFT Start")
        self._logger.info("HFT start")
        # 同时运行 task_1 和 task_2
        await asyncio.gather(self._bn_ws_api.start_ws(), self._bitget_ws_api.start_ws(), self.start_hft())  # 创建任务

    async def start_hft(self):
        last_time = 0
        last_bn_update_id = -1
        await self.check_position()
        while True:
            try:
                last_time = await self.pace_cycle_async(last_time, cyc_time=0.004)  # 4ms
                if (last_bn_update_id == self._bn_ws_api.update_id) or (self._bitget_ws_api.update_id == -1):
                    if time.time() - self._bitget_ws_api.last_ping_time > 30:  # 30s ping一次
                        await self._bitget_ws_api.ws_client.send("ping")
                        self._bitget_ws_api.last_ping_time = time.time()
                    continue
                if time.time() - self._bitget_ws_api.update_id > 180: # 3min没有更新
                    self.send_wechat(self._mail_to, 'Bitget数据长时间未更新', self._bitget_ws_api.update_id)
                    # 重建http连接
                    await asyncio.sleep(10)
                    continue
                self.analysis_bitget_ws_one()
                self.analysis_bn_bs_one()
                if self._bn_price_changed and (self._have_placed_order == 0.) and (self.v_volume_rate.value >1.) and len(self._bb_price_list) > 3:
                    if (0.3 > self.v_trade_side.value >= 0.) and (self._bn_ask_one / self._bitget_bid_one < self._sell_profit_rate):
                        self._have_placed_order = last_time
                        open_result = self._rest_api.make_open_order(p_price=self._bitget_bid_one, p_vol=self._order_vol, p_side="sell", p_client_id=self._client_open_order_id)
                        self._open_position_price = self._bitget_bid_one
                        self._open_position_side = -1  # sell
                        self._logger.info(json.dumps(open_result))
                        self.update_close_client_order_id()
                        self._last_close_price = 0.
                        self._logger.info(f"开空:{self._bitget_bid_one} BN ask:{self._bn_ask_one}, bid:{self._bn_bid_one}, Bitget ask:{self._bitget_ask_one}, bid:{self._bitget_bid_one}")
                        self.send_wechat(self._mail_to, "HFT开空", self._bitget_bid_one)
                        await self.cancel_client_order()
                    elif (self.v_trade_side.value > 0.7) and (self._bn_bid_one / self._bitget_ask_one > self._buy_profit_rate):
                        self._have_placed_order = last_time
                        open_result = self._rest_api.make_open_order(p_price=self._bitget_ask_one, p_vol=self._order_vol, p_side="buy", p_client_id=self._client_open_order_id)
                        self._open_position_price = self._bitget_ask_one
                        self._open_position_side = 1  # buy
                        self._logger.info(json.dumps(open_result))
                        self.update_close_client_order_id()
                        self._last_close_price = sys.maxsize
                        self._logger.info(f"开多：{self._bitget_ask_one} BN ask:{self._bn_ask_one}, bid:{self._bn_bid_one}, Bitget ask:{self._bitget_ask_one}, bid:{self._bitget_bid_one}")
                        self.send_wechat(self._mail_to, "HFT开多", self._bitget_ask_one)
                        await self.cancel_client_order()
                last_bn_update_id = self._bn_ws_api.update_id
                self.update_price_rate()
                await self.close_position()
                self.update_order_vol()
            except (socket.timeout, http.client.RemoteDisconnected, http.client.CannotSendRequest)  as e:
                err_msg = repr(e)
                self._logger.error(err_msg)
                self.send_wechat(self._mail_to, "HFT Exception1", err_msg)
                self._rest_api.init_https_connection()
            except Exception as e:
                error_info = "%s,%s" % (e, traceback.format_exc())
                self._logger.error(error_info)
                self.send_wechat(self._mail_to, 'HFT Exception2', error_info)
                # 重建http连接
                self._rest_api.init_https_connection()
                await asyncio.sleep(5)

    def analysis_bn_bs_one(self):
        val = orjson.loads(self._bn_ws_api.ws_message)
        data = val['data']
        _bn_ask_one = float(data['a'])
        if _bn_ask_one != self._bn_ask_one:
            self._bn_price_changed = True
            self._bn_ask_one = _bn_ask_one
        else:
            self._bn_price_changed = False

        self._bn_bid_one = float(data['b'])

    def analysis_bitget_ws_one(self):
        if self._bitget_ws_api.ws_message[0] == 'p':
            return # 'pong'
        val = orjson.loads(self._bitget_ws_api.ws_message)
        data = val['data'][0]
        self._bitget_ask_one = float(data['asks'][0][0])
        self._bitget_bid_one = float(data['bids'][0][0])

    def update_price_rate(self):
        if time.time() - self._last_price_list_update_time < self._price_rate_update_interval:  # 2.5min记一次
            return
        self._last_price_list_update_time = time.time()
        self._rest_api.init_https_connection()
        if len(self._bb_price_list) > self._bb_price_list_max_len:  # 1天
            old = self._bb_price_list.pop(0)
            self.sum_bn_sell -= old[0]
            self.sum_bn_buy -= old[1]
            self.sum_bitget_sell -= old[2]
            self.sum_bitget_buy -= old[3]

        self.sum_bn_sell += self._bn_ask_one
        self.sum_bn_buy += self._bn_bid_one
        self.sum_bitget_sell += self._bitget_ask_one
        self.sum_bitget_buy += self._bitget_bid_one

        self._bb_price_list.append((self._bn_ask_one, self._bn_bid_one, self._bitget_ask_one, self._bitget_bid_one))
        self._logger.info(f"价格列表长度：{len(self._bb_price_list)}")

        self._sell_profit_rate = self.sum_bn_sell / self.sum_bitget_buy
        self._buy_profit_rate = self.sum_bn_buy / self.sum_bitget_sell

    def update_order_vol(self):
        if self._have_placed_order != 0.:  # 下单了, 则不更新
            return
        self._order_vol = self.floor(ORDER_AMOUNT / self._bn_ask_one, self._pre_accuracy)
        self._client_open_order_id = f"{self._coin}{int(time.time()*1000)}"

    async def cancel_client_order(self):
        await asyncio.sleep(3)
        for _ in range(2):
            await asyncio.sleep(1)
            result = self._rest_api.cancel_order(self._client_open_order_id)
            self._logger.error(json.dumps(result))

    async def close_position(self):
        """
        平仓
        :return:
        """
        if self._have_placed_order == 0.:
            return # 没有下单, 没有仓位
        if time.time() - self._have_placed_order < self._close_position_delta_time:
            return
        posi_vol, _ = await self.analysis_position_info()
        if posi_vol is None:
            await asyncio.sleep(0.1)
            return
        if posi_vol == '0':
            self._logger.info("当前没有持仓")
            self._have_placed_order = 0.
            await self.dormant_after_closing_position()
            return
        if self._open_position_side == 1:
            # if self._last_close_price <= self._bitget_ask_one:
            #     self._logger.info(f"上一轮平仓价：{self._last_close_price} <= 卖一价：{self._bitget_ask_one}")
            #     await asyncio.sleep(0.3)
            #     return # 本次的价格并不优
            close_result = self._rest_api.make_close_order(p_price=self._bitget_ask_one, p_vol=posi_vol, p_side='sell', p_client_id=self._client_close_order_id)
            self._last_close_price = self._bitget_ask_one
        elif self._open_position_side == -1:
            # if self._last_close_price >= self._bitget_bid_one:
            #     self._logger.info(f"上一轮平仓价：{self._last_close_price} >= 买一价{self._bitget_bid_one}")
            #     await asyncio.sleep(0.3)
            #     return # 本次的价格并不优
            close_result = self._rest_api.make_close_order(p_price=self._bitget_bid_one, p_vol=posi_vol, p_side='buy', p_client_id=self._client_close_order_id)
            self._last_close_price = self._bitget_bid_one
        else:
            error_msg = f"异常的仓位方向:{self._open_position_side}, 持仓量:{posi_vol}"
            self._logger.error(error_msg)
            raise Exception(error_msg)
        # self._logger.info(f"平仓[{self._last_close_price}]：{json.dumps(close_result)}")
        self.analysis_close_position_result(close_result)
        await asyncio.sleep(0.3)
        cancel_result = self._rest_api.cancel_order(self._client_close_order_id)
        if not cancel_result:
            cancel_result = self._rest_api.cancel_order(self._client_close_order_id)
        # self._logger.info(json.dumps(cancel_result))
        self.update_close_client_order_id()
        await asyncio.sleep(1.5)

    async def dormant_after_closing_position(self):
        """
        平仓后休眠 4min (之前是2min)
        :return:
        """
        await asyncio.sleep(60)
        await self._bitget_ws_api.ws_client.send("ping")
        await asyncio.sleep(60)
        await self._bitget_ws_api.ws_client.send("ping")
        await asyncio.sleep(60)
        await self._bitget_ws_api.ws_client.send("ping")
        await asyncio.sleep(60)

    def start_stop_loss(self):
        """
        开始止损
        :return:
        """
        stop_loss_rate = 0.005
        if self._open_position_side == 1:  # long
            stop_loss_price = self._open_position_price * (1 - stop_loss_rate)
            if stop_loss_price < self._bitget_ask_one:
                return
            # 开始止损
        elif self._open_position_side == -1:  # short
            stop_loss_price = self._open_position_price * (1 + stop_loss_rate)
            if stop_loss_price > self._bitget_bid_one:
                return
            # 开始止损

    def analysis_close_position_result(self, result):
        if result is None:
            self._logger.error("平仓结果为空")
            return
        code = result['code']
        if code == '00000':
            return
        self._logger.error("平仓异常：" + json.dumps(result))
        self.send_wechat(self._mail_to, "平仓异常", result)

    async def analysis_position_info(self):
        positon_info = self._rest_api.get_position_info()
        # self._logger.info(json.dumps(positon_info))
        if not positon_info:
            self._logger.error("获取交易对仓位失败......")
            return None, None

        posi_code = positon_info['code']
        if posi_code == '429':  # 限流
            await asyncio.sleep(1)
            return None, None

        if posi_code != '00000':
            self.send_wechat(self._mail_to, "仓位接口异常", positon_info)
            await asyncio.sleep(3)
            return None, None

        data = positon_info['data']
        if data == []:
            self._open_position_side = 0
            self._open_position_price = None
            return '0', '0'
        position = data[0]
        self._open_position_price = float(position['openPriceAvg'])
        if position['holdSide'] == 'long':
            self._open_position_side = 1
            return position['total'], float(position['liquidationPrice'])
        else:
            self._open_position_side = -1
            return position['total'], float(position['liquidationPrice'])

    async def check_position(self):
        while True:
            try:
                posi_vol, _ = await self.analysis_position_info()
                if posi_vol is None:
                    self._logger.error(f"获取[{self._symbol}]失败")
                    await asyncio.sleep(0.5)
                    continue
                if posi_vol == '0':
                    self._logger.info("检查仓位：当前没有持仓")
                    self._have_placed_order = 0.
                    return
                self._have_placed_order = 1
                return
            except Exception as e:
                error_info = "%s,%s" % (e, traceback.format_exc())
                self._logger.error(error_info)
                await asyncio.sleep(1.5)

    def update_close_client_order_id(self):
        self._client_close_order_id = f"{self._coin}{int(time.time()*1000)}c"

