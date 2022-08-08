"""
Start and re-start processes, keep track of crashes and failed tasks
"""

import argparse
import asyncio
import logging
import signal
import time
import zmq

import galp.worker
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.reply_protocol import ReplyProtocol
from galp.async_utils import background

class Pool:
    """
    A pool of worker processes.
    """
    def __init__(self, args):
        self.args = args
        self.tasks = []
        self.signal = None
        self.pending_signal = asyncio.Event()
        self.last_start_time = time.time()

        self.broker_protocol = ReplyProtocol('BK', router=False)
        self.broker_transport = ZmqAsyncTransport(
            self.broker_protocol,
            args.endpoint, zmq.DEALER # pylint: disable=no-member
            )

    def set_signal(self, sig):
        """
        Synchronously marks that a signal is pending
        """
        self.signal = sig
        self.pending_signal.set()

    async def run(self):
        """
        Start one task to create and monitor each worker, along with the
        listening loop
        """

        self.tasks = [
            asyncio.create_task(
                self.start_worker()
            )
            for i in range(self.args.pool_size)
        ]
        async with background(
            self.broker_transport.listen_reply_loop()
            ):
            await asyncio.gather(*self.tasks)

    async def start_worker(self):
        """
        Starts a worker.
        """
        ret = True
        while ret and not self.pending_signal.is_set():
            self.last_start_time = time.time()
            pid = galp.worker.fork(**vars(self.args))

            logging.info('Started worker %d', pid)

            wait_signal = asyncio.create_task(self.pending_signal.wait())

            # FIXME: how to wait after a fork ?
            # wait_process = asyncio.create_task(process.wait())
            done, pending = await asyncio.wait(
                (wait_signal,),
                return_when=asyncio.FIRST_COMPLETED)

            if wait_signal in done:
                #if wait_process in pending:
                #    logging.error("Forwarding signal %d to worker %d",
                #        self.signal, process.pid)
                #    process.send_signal(self.signal)
                #    await process.wait()
                #else:
                    logging.info("Not forwarding to untracked worker %d", pid)
#            else:
#                ret = wait_process.result()
#                if ret:
#                    delay = self.last_start_time - time.time() + args.restart_delay
#                    delay = max(args.min_restart_delay, delay)
#                    logging.error(
#                        "Child %d exited with abnormal status %d, "
#                        "scheduling restart in at least %d seconds",
#                        process.pid, ret, delay
#                    )
#                    await self.notify_exit(process.pid)
#                    while delay > 0:
#                        wait_delay = asyncio.create_task(asyncio.sleep(delay))
#                        done, pending = await asyncio.wait(
#                            (wait_signal, wait_delay),
#                            return_when=asyncio.FIRST_COMPLETED)
#                        if wait_signal in done:
#                            logging.info("Not forwarding to dead worker %d", process.pid)
#                            break
#                        # We have waited the min time, now loop and start asap
#                        delay = self.last_start_time - time.time() + args.restart_delay
#                        if delay > 0:
#                            logging.error(
#                                "Postponing restart due to rate limiting, "
#                                "next restart in at least %d seconds",
#                                delay
#                            )
#                else:
#                    logging.info('Child %d exited normally, not restarting',
#                        process.pid)

    async def notify_exit(self, pid):
        """
        Sends a message back to broker to signal a worker died
        """
        await self.broker_transport.send_message(
            self.broker_protocol.exited(
                self.broker_protocol.default_route(), str(pid).encode('ascii')
                )
            )

def on_signal(sig, pool):
    """
    Signal handler, propagates it to the pool
    """
    logging.error("Caught signal %d (%s)", sig, signal.strsignal(sig))
    pool.set_signal(sig)

async def main(args):
    """
    Main CLI entry point
    """
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
    """
    Pool-specific arguments
    """
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
    # Convenience hook to start a pool from CLI
    _parser = argparse.ArgumentParser()
    add_parser_arguments(_parser)
    asyncio.run(main(_parser.parse_args()))
