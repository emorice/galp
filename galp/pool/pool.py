"""
Start and re-start processes, keep track of crashes and failed tasks
"""

import os
import asyncio
import logging
import signal
import threading
from contextlib import asynccontextmanager

import psutil
import zmq

import galp.worker
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.reply_protocol import ReplyProtocol
from galp.async_utils import background

class Pool:
    """
    A pool of worker processes.
    """
    def __init__(self, config, worker_config):
        self.config = config
        self.worker_config = worker_config
        self.pids = set()
        self.signal = None
        self.pending_signal = asyncio.Event()

        self.broker_protocol = ReplyProtocol('BK', router=False)
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
                async with run_forkserver(self.config, self.worker_config) as forkserver_socket:
                    self.forkserver_socket = forkserver_socket
                    for _ in range(self.config['pool_size']):
                        self.start_worker()
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

    def start_worker(self):
        """
        Starts a worker.
        """
        self.forkserver_socket.send(b'FORK')
        b_pid = self.forkserver_socket.recv()
        pid = int.from_bytes(b_pid, 'little')

        logging.info('Started worker %d', pid)

        self.pids.add(pid)

    async def notify_exit(self, pid):
        """
        Sends a message back to broker to signal a worker died
        """
        await self.broker_transport.send_message(
            self.broker_protocol.exited(
                self.broker_protocol.default_route(), str(pid).encode('ascii')
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
                logging.error('Worker %s exited with code %s',
                        rpid, rstatus >> 8)
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

def forkserver(config, worker_config):
    """
    A dedicated loop to fork workers on demand and return the pids.

    Start this in a clean new thread that does not have a running asyncio event
    loop or any state of the sort.
    """
    socket = zmq.Context.instance().socket(zmq.PAIR) # pylint: disable=no-member
    socket.connect('inproc://galp_forkserver')

    if config.get('pin_workers'):
        cpus = psutil.Process().cpu_affinity()
        cpu_counter = 0

    while True:
        msg = socket.recv()
        if msg == b'FORK':
            if config.get('pin_workers'):
                cpu = cpus[cpu_counter]
                cpu_counter = (cpu_counter + 1) % len(cpus)
                logging.info('Pinning new worker to cpu %d', cpu)
                pid = galp.worker.fork(worker_config, pin_cpus=[cpu])
            else:
                pid = galp.worker.fork(worker_config)
            socket.send(pid.to_bytes(4, 'little'))
        else:
            break

@asynccontextmanager
async def run_forkserver(config, worker_config):
    """
    Async context handler to start a forkserver thread.
    """
    socket = zmq.Context.instance().socket(zmq.PAIR) # pylint: disable=no-member
    socket.bind('inproc://galp_forkserver')

    thread = threading.Thread(target=forkserver, args=(config, worker_config))
    thread.start()

    try:
        yield socket
    finally:
        socket.send(b'EXIT')
        thread.join()
