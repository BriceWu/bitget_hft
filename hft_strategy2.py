#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
import time
import asyncio
from hft_strategy import HFTStrategy


class HFTStrategyTWO(HFTStrategy):

    async def close_position(self):
        """
        平仓
        :return:
        """
        if self._have_placed_order == 0.:
            return # 没有下单, 没有仓位
        delta_time = time.time() - self._have_placed_order
        if delta_time < 10:
            return
        posi_side, posi_vol, avg_price, liq_price = await self.analysis_position_info()
        if posi_vol is None:
            await asyncio.sleep(0.1)
            return
        if posi_vol == '0':
            self._logger.info("当前没有持仓")
            self._have_placed_order = 0.
            await self.dormant_after_closing_position()
            return
        if posi_side == 1:  # 做多
            if delta_time > self._close_position_delta_time:
                close_result = self._rest_api.make_close_order(p_price=self._bitget_ask_one, p_vol=posi_vol, p_side='sell', p_client_id=self._client_close_order_id)
                self._last_close_price = self._bitget_ask_one
            else:
                new_price = avg_price * (1 + self.get_profit_ratio(delta_time))
        elif posi_side == -1:  # 做空
            if delta_time > self._close_position_delta_time:
                close_result = self._rest_api.make_close_order(p_price=self._bitget_bid_one, p_vol=posi_vol, p_side='buy', p_client_id=self._client_close_order_id)
                self._last_close_price = self._bitget_bid_one
            else:
                pass
        else:
            error_msg = f"异常的仓位方向:{posi_side}, 持仓量:{posi_vol}"
            self._logger.error(error_msg)
            raise Exception(error_msg)
        self.analysis_close_position_result(close_result)
        await asyncio.sleep(0.3)
        cancel_result = self._rest_api.cancel_order(self._client_close_order_id)
        if not cancel_result:
            cancel_result = self._rest_api.cancel_order(self._client_close_order_id)
        self.update_close_client_order_id()
        await asyncio.sleep(1.5)

    def get_profit_ratio(self, delta_time):
        rate = min(delta_time / self._close_position_delta_time, 1)
        return 0.02 - 0.018 * rate

