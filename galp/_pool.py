"""
Start and re-start processes, keep track of crashes and failed tasks
"""

import os
import sys
import asyncio
import logging
import signal
import threading
from contextlib import asynccontextmanager
import subprocess

import psutil
import zmq

import galp.worker
import galp.messages as gm
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.reply_protocol import ReplyProtocol
from galp.serializer import dump_model, load_model
from galp.async_utils import background

class Pool:
    """
    A pool of worker processes.

    Args:
        config: pool and worker config. There is no distinction between the two,
            any worker-specific option can be given to the pool and will be
            forwarded to the workers; and pool-specific options like pool size
            are still visible to the worker.
    """
    def __init__(self, config):
        self.config = config
        self.pids = set()
        self.signal = None
        self.pending_signal = asyncio.Event()

        self.broker_protocol = BrokerProtocol(pool=self)
        self.broker_transport = ZmqAsyncTransport(
            self.broker_protocol,
            config['endpoint'], zmq.DEALER # pylint: disable=no-member
            )

        self.forkserver_socket = None

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
        try:
            async with background(
                self.broker_transport.listen_reply_loop()
                ):
                async with run_forkserver(self.config) as forkserver_socket:
                    self.forkserver_socket = forkserver_socket

                    await self.notify_ready()
                    while True:
                        await self.pending_signal.wait()
                        sig = self.signal
                        if sig == signal.SIGCHLD:
                            await self.check_deaths()
                            self.pending_signal.clear()
                        else:
                            self.kill_all(sig)
                            break
        except asyncio.CancelledError:
            self.kill_all()
            raise

    def start_worker(self, fork_msg: gm.Fork):
        """
        Starts a worker.
        """
        self.forkserver_socket.send(dump_model(fork_msg))
        b_pid = self.forkserver_socket.recv()
        pid = int.from_bytes(b_pid, 'little')

        logging.info('Started worker %d', pid)

        self.pids.add(pid)

    async def notify_exit(self, pid):
        """
        Sends a message back to broker to signal a worker died
        """
        await self.broker_transport.send_message(
                self.broker_protocol.route_message(None,
                    gm.Exited(peer=str(pid))
                )
            )

    async def notify_ready(self):
        """
        Sends a message back to broker to signal we joined, and with which cpus
        """
        cpus = list(psutil.Process().cpu_affinity())
        await self.broker_transport.send_message(
                self.broker_protocol.route_message(None,
                    gm.PoolReady(cpus=cpus)
                    )
                )

    async def check_deaths(self):
        """
        Check for children deaths and send messages to broker
        """
        rpid = True
        while self.pids and rpid:
            rpid, rstatus = os.waitpid(-1, os.WNOHANG)
            if rpid:
                if rpid not in self.pids:
                    logging.error('Ignoring exit of unknown child %s', rpid)
                    continue
                rsig = rstatus & 0x7F
                rdumped = rstatus & 0x80
                rret = rstatus >> 8
                if rsig:
                    logging.error('Worker %s killed by signal %d (%s) %s', rpid,
                            rsig,
                            signal.strsignal(rsig),
                            '(core dumped)' if rdumped else ''
                            )
                else:
                    logging.error('Worker %s exited with code %s',
                            rpid, rret)
                await self.notify_exit(rpid)
                self.pids.remove(rpid)

    def kill_all(self, sig=signal.SIGTERM):
        """
        Kill all children and wait for them
        """
        logging.info('Sending %s to all remaining %s children',
                sig, len(self.pids))
        for pid in self.pids:
            logging.debug('Sending %s to %s', sig, pid)
            os.kill(pid, sig)
            logging.debug('Sent %s to %s', sig, pid)
        for pid in self.pids:
            logging.info('Waiting for %s', pid)
            rpid = 0
            while not rpid:
                rpid, rexit = os.waitpid(pid, 0)
                logging.info('Child %s exited with code %d', pid, rexit >> 8)

class BrokerProtocol(ReplyProtocol):
    """
    Simple protocol to process spawn request from broker
    """
    def __init__(self, pool: Pool) -> None:
        super().__init__('BK', router=False)
        self.pool = pool

    def on_fork(self, msg: gm.Fork):
        """
        Request to spawn a worker for a task
        """
        self.pool.start_worker(msg)

def forkserver(config):
    """
    A dedicated loop to fork workers on demand and return the pids.

    Start this in a clean new thread that does not have a running asyncio event
    loop or any state of the sort.
    """
    socket = zmq.Context.instance().socket(zmq.PAIR) # pylint: disable=no-member
    socket.connect('inproc://galp_forkserver')

    if config.get('pin_workers'):
        cpus = psutil.Process().cpu_affinity()

    while True:
        msg = load_model(gm.Message, socket.recv())
        match msg:
            case gm.Fork():
                _config = dict(config, mission=msg.mission,
                               cpus_per_task=msg.resources.cpus)
                if _config.get('pin_workers'):
                    # We always pin to the first n cpus. This does not mean that
                    # we will actually execute on these ; cpu pins should be
                    # reset in the worker based on information from the broker
                    # at each task. Rather, it is a matter of having the right
                    # number of bits in the cpu mask when modules that inspect
                    # the mask are loaded, possibly even before the first task
                    # is run.
                    _cpus = cpus[:msg.resources.cpus]
                    logging.info('Pinning new worker to cpus %s', _cpus)
                    pid = galp.worker.fork(dict(_config, pin_cpus=_cpus))
                else:
                    pid = galp.worker.fork(_config)
                socket.send(pid.to_bytes(4, 'little'))
            case gm.Exit():
                break

@asynccontextmanager
async def run_forkserver(config):
    """
    Async context handler to start a forkserver thread.
    """
    socket = zmq.Context.instance().socket(zmq.PAIR) # pylint: disable=no-member
    socket.bind('inproc://galp_forkserver')

    thread = threading.Thread(target=forkserver, args=(config,))
    thread.start()

    try:
        yield socket
    finally:
        socket.send(dump_model(gm.Exit()))
        thread.join()

def on_signal(sig, pool):
    """
    Signal handler, propagates it to the pool

    Does not handle race conditions where a signal is received before the
    previous is handled, but that should not be a problem: CHLD always does the
    same thing and TERM or INT would supersede CHLD.
    """
    logging.info("Caught signal %d (%s)", sig, signal.strsignal(sig))
    pool.set_signal(sig)

def main(config):
    """
    Main process entry point
    """
    galp.cli.setup(" pool ", config.get('log_level'))
    logging.info("Starting worker pool")

    async def _amain(config):
        pool = Pool(config)

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGCHLD):
            loop.add_signal_handler(sig,
                lambda sig=sig, pool=pool: on_signal(sig, pool)
            )

        await pool.run()

        logging.info("Pool manager exiting")
    asyncio.run(_amain(config))
    return 0

def make_cli(config):
    """
    Generate a command line to spawn a pool
    """
    args = []
    for key, val in config.items():
        match key:
            case 'pin_workers':
                if val:
                    args.append('--pin_workers')
            case 'endpoint':
                args.append(val)
            case 'store':
                args.append(val)
            case 'steps':
                for step in val or []:
                    args.append('--steps')
                    args.append(step)
            case 'log_level':
                args.append('--log-level')
                args.append(val)
            case 'config':
                if val:
                    args.append('--config')
                    args.append(val)
            case _:
                raise ValueError(f'Argument {key} is not supported by the pool'
                    'spawn entry point')
    return [str(obj) for obj in args]

def spawn(config):
    """
    Spawn a new pool process
    """
    return subprocess.Popen([sys.executable, '-m', 'galp.pool'] +
            make_cli(config))
