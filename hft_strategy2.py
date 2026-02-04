#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
import time
import asyncio
from hft_strategy import HFTStrategy


class HFTStrategyTWO(HFTStrategy):
    def __init__(self, symbol, mark, volume_rate, trade_side):
        """
        初始化
        :param symbol: 交易对
        """
        HFTStrategy.__init__(self, symbol, mark, volume_rate, trade_side)

    async def close_position(self):
        """
        平仓
        :return:
        """
        if self._have_placed_order == 0.:
            return # 没有下单, 没有仓位
        if time.time() - self._have_placed_order < 4.5*60:  # 2min
            return
        posi_vol, posi_side = await self.analysis_position_info()
        if posi_vol is None:
            await asyncio.sleep(0.1)
            return
        if posi_vol == '0':
            self._logger.info("当前没有持仓")
            self._have_placed_order = 0.
            await self.dormant_after_closing_position()
            return
        if posi_side == 1:
            # if self._last_close_price <= self._bitget_ask_one:
            #     self._logger.info(f"上一轮平仓价：{self._last_close_price} <= 卖一价：{self._bitget_ask_one}")
            #     await asyncio.sleep(0.3)
            #     return # 本次的价格并不优
            close_result = self._rest_api.make_close_order(p_price=self._bitget_ask_one, p_vol=posi_vol, p_side='sell', p_client_id=self._client_close_order_id)
            self._last_close_price = self._bitget_ask_one
        elif posi_side == -1:
            # if self._last_close_price >= self._bitget_bid_one:
            #     self._logger.info(f"上一轮平仓价：{self._last_close_price} >= 买一价{self._bitget_bid_one}")
            #     await asyncio.sleep(0.3)
            #     return # 本次的价格并不优
            close_result = self._rest_api.make_close_order(p_price=self._bitget_bid_one, p_vol=posi_vol, p_side='buy', p_client_id=self._client_close_order_id)
            self._last_close_price = self._bitget_bid_one
        else:
            error_msg = f"异常的仓位方向:{posi_side}, 持仓量:{posi_vol}"
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

