#!/usr/bin/python3.13
# -*- coding:utf-8 -*-
from sys import argv
import asyncio
from multiprocessing import Process, Value
from zmqfsi.util.zm_env import RunEnv

from volume_monitor import VolumeMonitor

from hft_strategy import HFTStrategy

def __volume_monitor(env, symbol, volume_rate, trade_side):
    RunEnv.set_run_env(env)
    vm = VolumeMonitor(symbol, volume_rate, trade_side)
    vm.start()

if __name__ == '__main__':
    RunEnv.set_run_env(argv[1])
    _env = RunEnv.get_run_env()
    _symbol = argv[2]
    _mark = "xyz369free"
    v_trade_side = Value('d', 0)
    v_volume_rate = Value('d', 0)
    Process(target=__volume_monitor, args=(_env, _symbol, v_volume_rate, v_trade_side)).start()
    loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(loop)
    active = HFTStrategy(_symbol, _mark, v_volume_rate, v_trade_side)
    loop.run_until_complete(active.run_tasks())