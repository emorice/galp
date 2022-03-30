"""
Start and re-start processes, keep track of crashes and failed tasks
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
import time
import zmq

import galp.worker
from galp.zmq_async_protocol import ZmqAsyncProtocol

class Pool:
    def __init__(self, args):
        self.args = args
        self.tasks = []
        self.pending_signal = asyncio.Event()
        self.last_start_time = time.time()

        self.broker = ZmqAsyncProtocol('BK', args.endpoint, zmq.DEALER)

    def set_signal(self, sig):
        self.signal = sig
        self.pending_signal.set() 
        
    async def run(self):
        self.tasks = [
            asyncio.create_task(
                self.start_worker()
            )
            for i in range(self.args.pool_size)
        ]
        await asyncio.gather(*self.tasks)
        
    async def start_worker(self):
        args = self.args
        arg_list = ['-m', 'galp.worker']
        if args.config:
            arg_list.extend(['-c', args.config])
        if args.debug:
            arg_list.extend(['--debug'])
        arg_list.extend([args.endpoint, args.cachedir])

        ret = True
        while ret and not self.pending_signal.is_set():
            self.last_start_time = time.time()
            process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    *arg_list,
                )
            logging.info('Started worker %d', process.pid)

            wait_signal = asyncio.create_task(self.pending_signal.wait())
            wait_process = asyncio.create_task(process.wait())
            done, pending = await asyncio.wait(
                (wait_signal, wait_process),
                return_when=asyncio.FIRST_COMPLETED)
    
            if wait_signal in done:
                if wait_process in pending:
                    logging.error("Forwarding signal %d to worker %d",
                        self.signal, process.pid)
                    process.send_signal(self.signal)
                    await process.wait()
                else:
                    logging.info("Not forwarding to dead worker %d", process.pid)
            else:
                ret = wait_process.result() 
                if ret:
                    delay = self.last_start_time - time.time() + args.restart_delay
                    delay = max(args.min_restart_delay, delay)
                    logging.error(
                        "Child %d exited with abnormal status %d, "
                        "scheduling restart in at least %d seconds",
                        process.pid, ret, delay
                    )
                    await self.notify_exit(process.pid)
                    while delay > 0:
                        wait_delay = asyncio.create_task(asyncio.sleep(delay))
                        done, pending = await asyncio.wait(
                            (wait_signal, wait_delay),
                            return_when=asyncio.FIRST_COMPLETED)
                        if wait_signal in done:
                            logging.info("Not forwarding to dead worker %d", process.pid)
                            break
                        else:
                            # We have waited the min time, now loop and start asap
                            delay = self.last_start_time - time.time() + args.restart_delay
                            if delay > 0:
                                logging.error(
                                    "Postponing restart due to rate limiting, "
                                    "next restart in at least %d seconds",
                                    delay
                                )
                else:
                    logging.info('Child %d exited normally, not restarting',
                        process.pid)

    async def notify_exit(self, pid):
        await self.broker.exited([], str(pid).encode('ascii'))

def on_signal(sig, pool):
    logging.error("Caught signal %d (%s)", sig, signal.strsignal(sig))
    pool.set_signal(sig)

async def main(args):
    galp.cli.setup(args, " pool ")
    logging.info("Starting worker pool")

    pool = Pool(args)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig,
            lambda sig=sig, pool=pool: on_signal(sig, pool)
        )
     
    await pool.run() 

    logging.info("Pool manager exiting")
        
def add_parser_arguments(parser):
    parser.add_argument('pool_size', type=int,
        help='Number of workers to start')
    parser.add_argument('--restart_delay', type=int,
        help='Do not restart more than one failed worker '
        'per this interval of time (s)',
        default=3600
        )
    parser.add_argument('--min_restart_delay', type=int,
        help='Wait at least this time (s) before restarting a failed worker',
        default=300
        )
    galp.worker.add_parser_arguments(parser)

if __name__ == '__main__':
    """Convenience hook to start a pool from CLI""" 
    parser = argparse.ArgumentParser()
    add_parser_arguments(parser)
    asyncio.run(main(parser.parse_args()))
