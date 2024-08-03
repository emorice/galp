"""
Start and re-start processes, keep track of crashes and failed tasks
"""

import os
import sys
import asyncio
import logging
import signal
import threading
import selectors
from contextlib import contextmanager
import subprocess
import socket

import psutil
import zmq

import galp.worker
import galp.net.core.types as gm
import galp.socket_transport
from galp.result import Result, Ok, Error
from galp.zmq_async_transport import ZmqAsyncTransport, TransportMessage
from galp.protocol import make_stack
from galp.net.core.dump import dump_message
from galp.net.core.load import parse_core_message
from galp.async_utils import background

# Communication utils
# ===================

def socket_send_message(sock: socket.socket, message: gm.Message):
    """Serialize and send galp message over sock"""
    return galp.socket_transport.send_multipart(sock, dump_message(message))

async def async_socket_recv_message(sock: socket.socket) -> Result[gm.Message]:
    """Receive and deserialize galp message over sock"""
    return parse_core_message(await galp.socket_transport.async_recv_multipart(sock))

# Proxying
# ========

class Proxy:
    """
    Proxy between unix socket and zmq
    """
    def __init__(self, broker_endpoint: str, forkserver_socket: socket.socket):
        def on_message(_write, msg: gm.Message
                       ) -> list[TransportMessage] | Error:
            match msg:
                case gm.Fork():
                    socket_send_message(self.forkserver_socket, msg)
                    return []
                case _:
                    return Error(f'Unexpected {msg}')
        stack = make_stack(on_message, name='BK')
        self.broker_transport = ZmqAsyncTransport(stack,
            broker_endpoint, zmq.DEALER # pylint: disable=no-member
            )

        forkserver_socket.setblocking(False)
        self.forkserver_socket = forkserver_socket

    # Main
    async def run(self):
        """
        Start one task to create and monitor each worker, along with the
        listening loop
        """
        cpus = list(psutil.Process().cpu_affinity())
        await self.broker_transport.send_message(gm.PoolReady(cpus=cpus))

        async with background(
            self.broker_transport.listen_reply_loop()
            ):
            await self.listen_forkserver()

    # Proxy
    async def listen_forkserver(self) -> None:
        """Process messages from the forkserver"""
        while True:
            try:
                message = await async_socket_recv_message(self.forkserver_socket)
            except EOFError:
                return
            match message:
                case Ok(_message):
                    # Forward anything else to broker
                    await self.broker_transport.send_message(_message)
                case _:
                    raise ValueError(f'Unexpected {message}')

# Child process management
# ========================

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

def on_socket_frames(frames, config, cpus, sock_server, pids):
    """
    Handler for messages proxied to forkserver
    """
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
            socket_send_message(sock_server, gm.Ready(str(pid), b''))
            pids.add(pid)
        case _:
            logging.error('Unexpected %s', msg)

def register_socket_handler(selector, config, sock_server, pids):
    """
    Initialize the handler for broker messages, which does the actual forking
    work
    """
    cpus = []
    if config.get('pin_workers'):
        cpus = psutil.Process().cpu_affinity()
        assert cpus

    # Set up protocol
    multipart_reader = galp.socket_transport.make_multipart_generator(
            lambda frames: on_socket_frames(frames, config, cpus, sock_server,
                                            pids)
            )
    multipart_reader.send(None)

    # Connect transport
    def _on_socket():
        buf = sock_server.recv(4096)
        if buf:
            multipart_reader.send(buf)
            return False
        # Disconnect
        return True
    selector.register(sock_server, selectors.EVENT_READ, _on_socket)

def check_deaths(pids: set[int], sock_server: socket.socket) -> None:
    """
    Check for children deaths and send messages to broker
    """
    rpid = -1
    while pids and rpid:
        rpid, rstatus = os.waitpid(-1, os.WNOHANG)
        if rpid:
            if rpid not in pids:
                logging.error('Ignoring exit of unknown child %s', rpid)
                continue
            log_child_exit(rpid, rstatus, logging.ERROR)
            socket_send_message(sock_server, gm.Exited(peer=str(rpid)))
            pids.remove(rpid)

def kill_all(pids: set[int], sig: signal.Signals = signal.SIGTERM) -> None:
    """
    Kill all children and wait for them
    """
    logging.info('Sending %s to all remaining %s children',
            sig, len(pids))
    for pid in pids:
        logging.debug('Sending %s to %s', sig, pid)
        os.kill(pid, sig)
        logging.debug('Sent %s to %s', sig, pid)
    for pid in pids:
        logging.info('Waiting for %s', pid)
        rpid = 0
        while not rpid:
            rpid, rexit = os.waitpid(pid, 0)
            log_child_exit(rpid, rexit)

def register_signal_handler(selector, pids: set[int], signal_read_fd,
                            sock_server) -> None:
    """
    Set up signal handler to monitor worker deaths
    """
    def _on_signal():
        sig_b = os.read(signal_read_fd, 1)
        sig = signal.Signals(int.from_bytes(sig_b, 'little'))
        logging.info('FK Received signal %s', sig)
        if sig == signal.SIGCHLD:
            check_deaths(pids, sock_server)
        elif sig in (signal.SIGINT, signal.SIGTERM):
            kill_all(pids, sig)
            return True
        else:
            logging.debug('Ignoring signal %s', sig)
        return False
    selector.register(signal_read_fd, selectors.EVENT_READ, _on_signal)

def forkserver(sock_server, signal_read_fd, config) -> None:
    """
    A dedicated loop to fork workers on demand and return the pids.

    Start this in a clean new thread that does not have a running asyncio event
    loop or any state of the sort.
    """
    selector = selectors.DefaultSelector()
    pids : set[int] = set()
    register_socket_handler(selector, config, sock_server, pids)
    register_signal_handler(selector, pids, signal_read_fd, sock_server)

    leave = False
    while not leave:
        events = selector.select()
        for key, _mask in events:
            callback = key.data
            leave = callback()

    sock_server.close()

@contextmanager
def run_forkserver(config, signal_read_fd):
    """
    Async context handler to start a forkserver thread.
    """
    sock_server, sock_client = socket.socketpair()

    thread = threading.Thread(target=forkserver, args=(sock_server,
                                                       signal_read_fd, config))
    thread.start()

    try:
        yield sock_client, sock_server
    finally:
        sock_client.close() # Forkserver will detect connection closed
        thread.join()

# Entry point
# ============

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

    with run_forkserver(config, signal_read_fd) as (forkserver_socket, _tmp_fk_sock):
        proxy = Proxy(config['endpoint'], forkserver_socket)
        asyncio.run(proxy.run())
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
