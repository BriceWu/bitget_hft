#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
from sys import argv
import asyncio
from multiprocessing import Process, Value
from zmqfsi.util.zm_env import RunEnv

from volume_monitor import VolumeMonitor
from hft_strategy import HFTStrategy

def __hft_strategy1(env, symbol, volume_rate, trade_side):
    RunEnv.set_run_env(env)
    loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(loop)
    _mark = "xyz369free"
    active = HFTStrategy(symbol, _mark, volume_rate, trade_side)
    loop.run_until_complete(active.run_tasks())

def __hft_strategy2(env, symbol, volume_rate, trade_side):
    RunEnv.set_run_env(env)
    loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(loop)
    _mark = "itbricewu"
    active = HFTStrategy(symbol, _mark, volume_rate, trade_side)
    loop.run_until_complete(active.run_tasks())

if __name__ == '__main__':
    RunEnv.set_run_env(argv[1])
    _env = RunEnv.get_run_env()
    _symbol = argv[2]

    v_trade_side = Value('d', 0)
    v_volume_rate = Value('d', 0)

    # Process(target=__hft_strategy1, args=(_env, _symbol, v_volume_rate, v_trade_side)).start()
    Process(target=__hft_strategy2, args=(_env, _symbol, v_volume_rate, v_trade_side)).start()
    vm = VolumeMonitor(_symbol, v_volume_rate, v_trade_side)
    vm.start()
