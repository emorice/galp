"""
Start and re-start processes, keep track of crashes and failed tasks
"""

import os
import sys
import asyncio
import logging
import signal
import threading
from contextlib import contextmanager
import subprocess
import socket

import psutil
import zmq

import galp.worker
import galp.net.core.types as gm
import galp.socket_transport as socket_transport
from galp.result import Ok, Error
from galp.zmq_async_transport import ZmqAsyncTransport, TransportMessage
from galp.protocol import make_stack
from galp.net.core.dump import dump_message
from galp.net.core.load import parse_core_message
from galp.async_utils import background

FORKSERVER_BUFSIZE = 4096

class Pool:
    """
    A pool of worker processes.

    Args:
        config: pool and worker config. There is no distinction between the two,
            any worker-specific option can be given to the pool and will be
            forwarded to the workers; and pool-specific options like pool size
            are still visible to the worker.
    """
    def __init__(self, broker_endpoint, signal_read_fd, forkserver_socket):
        self.pids = set()

        self._signal_read_fd = signal_read_fd

        def on_message(_write, msg: gm.Message
                ) -> list[TransportMessage] | Error:
            match msg:
                case gm.Fork():
                    self.start_worker(msg)
                    return []
                case _:
                    return Error(f'Unexpected {msg}')
        stack = make_stack(on_message, name='BK')
        self.broker_transport = ZmqAsyncTransport(stack,
            broker_endpoint, zmq.DEALER # pylint: disable=no-member
            )

        self.forkserver_socket = forkserver_socket


    async def run(self):
        """
        Start one task to create and monitor each worker, along with the
        listening loop
        """
        # Wrap pipe into an awaitable reader
        signal_reader = asyncio.StreamReader()
        await asyncio.get_event_loop().connect_read_pipe(
                lambda: asyncio.StreamReaderProtocol(signal_reader),
                open(self._signal_read_fd, 'rb', buffering=0) # pylint: disable=consider-using-with
                )
        try:
            async with background(
                self.broker_transport.listen_reply_loop()
                ):
                await self.notify_ready()
                while True:
                    _sig_b = await signal_reader.read(1)
                    sig = signal.Signals(int.from_bytes(_sig_b, 'little'))
                    logging.info('Received signal %s', sig)
                    if sig == signal.SIGCHLD:
                        await self.check_deaths()
                    elif sig in (signal.SIGINT, signal.SIGTERM):
                        self.kill_all(sig)
                        break
                    else:
                        logging.debug('Ignoring signal %s', sig)
        except asyncio.CancelledError:
            self.kill_all()
            raise

    def start_worker(self, fork_msg: gm.Fork):
        """
        Starts a worker.
        """
        frames = dump_message(fork_msg)
        socket_transport.send_multipart(self.forkserver_socket, frames)
        b_pid = self.forkserver_socket.recv(FORKSERVER_BUFSIZE)
        pid = int.from_bytes(b_pid, 'little')

        logging.info('Started worker %d', pid)

        self.pids.add(pid)

    async def notify_exit(self, pid):
        """
        Sends a message back to broker to signal a worker died
        """
        await self.broker_transport.send_message(gm.Exited(peer=str(pid)))

    async def notify_ready(self):
        """
        Sends a message back to broker to signal we joined, and with which cpus
        """
        cpus = list(psutil.Process().cpu_affinity())
        await self.broker_transport.send_message(gm.PoolReady(cpus=cpus))

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
                log_child_exit(rpid, rstatus, logging.ERROR)
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
                log_child_exit(rpid, rexit)

def log_child_exit(rpid, rstatus,
                   noerror_level=logging.INFO):
    """
    Parse and log exit status
    """
    rsig = rstatus & 0x7F
    rdumped = rstatus & 0x80
    rret = rstatus >> 8
    if rsig:
        if rsig in (signal.SIGTERM, signal.SIGINT):
            level = noerror_level
        else:
            level = logging.ERROR
        logging.log(level, 'Worker %s killed by signal %d (%s) %s', rpid,
                rsig,
                signal.strsignal(rsig),
                '(core dumped)' if rdumped else ''
                )
    else:
        logging.log(noerror_level, 'Worker %s exited with code %s',
                rpid, rret)

def forkserver(sock_server, config) -> None:
    """
    A dedicated loop to fork workers on demand and return the pids.

    Start this in a clean new thread that does not have a running asyncio event
    loop or any state of the sort.
    """
    if config.get('pin_workers'):
        cpus = psutil.Process().cpu_affinity() or []
        assert cpus

    while True:
        frames = socket_transport.recv_multipart(sock_server)
        msg = parse_core_message(frames)
        match msg:
            case Ok(gm.Fork() as fork):
                _config = dict(config, mission=fork.mission,
                               cpus_per_task=fork.resources.cpus)
                if _config.get('pin_workers'):
                    # We always pin to the first n cpus. This does not mean that
                    # we will actually execute on these ; cpu pins should be
                    # reset in the worker based on information from the broker
                    # at each task. Rather, it is a matter of having the right
                    # number of bits in the cpu mask when modules that inspect
                    # the mask are loaded, possibly even before the first task
                    # is run.
                    _cpus = cpus[:fork.resources.cpus]
                    logging.info('Pinning new worker to cpus %s', _cpus)
                    pid = galp.worker.fork(dict(_config, pin_cpus=_cpus))
                else:
                    pid = galp.worker.fork(_config)
                sock_server.sendall(pid.to_bytes(4, 'little'))
            case _:
                break
    sock_server.close()

@contextmanager
def run_forkserver(config):
    """
    Async context handler to start a forkserver thread.
    """
    sock_server, sock_client = socket.socketpair()

    thread = threading.Thread(target=forkserver, args=(sock_server, config))
    thread.start()

    try:
        yield sock_client
    finally:
        socket_transport.send_multipart(sock_client, [b''])
        sock_client.close()
        thread.join()

def main(config):
    """
    Main process entry point
    """
    galp.cli.setup(" pool ", config.get('log_level'))
    logging.info("Starting worker pool")

    # Self-pipe to receive signals
    # The write end is non-blocking, to make sure we get an error and not a
    # deadlock in the case (which should never happen) of signals filling
    # the pipe.
    # TODO: we should probably close the pipe after forking
    signal_read_fd, signal_write_fd = os.pipe()
    os.set_blocking(signal_write_fd, False)
    signal.set_wakeup_fd(signal_write_fd, warn_on_full_buffer=True)
    for sig in (signal.SIGCHLD, signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: None)

    with run_forkserver(config) as forkserver_socket:
        pool = Pool(config['endpoint'], signal_read_fd, forkserver_socket)
        asyncio.run(pool.run())
        logging.info("Pool manager exiting")

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
                if val:
                    args.append('--store')
                    args.append(val)
            case 'log_level':
                if val:
                    args.append('--log-level')
                    args.append(val)
            case 'vm':
                if val:
                    args.append('--vm')
                    args.append(val)
            case _:
                raise ValueError(f'Argument {key} is not supported by the pool'
                    'spawn entry point')
    return [str(obj) for obj in args]

def spawn(config):
    """
    Spawn a new pool process
    """
    line = [sys.executable, '-m', 'galp.pool'] + make_cli(config)
    logging.info('Spawning %s', line)
    return subprocess.Popen(line)
