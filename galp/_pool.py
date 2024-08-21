"""
Start and re-start processes, keep track of crashes and failed tasks
"""

import os
import sys
import asyncio
import logging
import signal
import selectors
import subprocess
import socket

import psutil
import zmq

import galp.worker
import galp.net.core.types as gm
import galp.socket_transport
import galp.logserver
from galp.result import Result, Ok
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.protocol import make_stack
from galp.net.core.dump import dump_message
from galp.net.core.load import parse_core_message
from galp.async_utils import background

# Communication utils
# ===================

def socket_send_message(sock: socket.socket, message: gm.Message) -> None:
    """Serialize and send galp message over sock"""
    return galp.socket_transport.send_multipart(sock, dump_message(message))

async def async_socket_recv_message(sock: socket.socket) -> Result[gm.Message]:
    """Receive and deserialize galp message over sock"""
    return parse_core_message(await galp.socket_transport.async_recv_multipart(sock))

# Proxying
# ========

async def proxy(broker_endpoint, forkserver_socket):
    """
    Proxy between unix socket and zmq.

    The proxy stops when the forkserver side closes the socket.
    """
    def on_message(_write, message):
        socket_send_message(forkserver_socket, message)
        return []
    stack = make_stack(on_message, name='BK')
    broker_transport = ZmqAsyncTransport(stack,
        broker_endpoint, zmq.DEALER # pylint: disable=no-member
        )

    async with background(
        broker_transport.listen_reply_loop()
        ):
        await listen_forkserver(forkserver_socket, broker_transport)

async def listen_forkserver(forkserver_socket, broker_transport) -> None:
    """Process messages from the forkserver"""
    forkserver_socket.setblocking(False)
    while True:
        try:
            message = await async_socket_recv_message(forkserver_socket)
        except EOFError:
            return
        match message:
            case Ok(_message):
                # Forward anything else to broker
                await broker_transport.send_message(_message)
            case _:
                raise ValueError(f'Unexpected {message}')

# Listen signals and monitor deaths
# =================================

def log_child_exit(rpid, rstatus,
                   noerror_level=logging.DEBUG):
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
        if rsig == signal.SIGKILL:
            try:
                # Check dmesg for OOM
                dmesg = subprocess.run(
                        f'dmesg -T | grep -i "Killed process {rpid}"',
                        shell=True, stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL, check=False)
                if dmesg.returncode == 0:
                    logging.log(level, 'Worker %s has a matching kernel log entry:', rpid)
                    logging.log(level, '%s', dmesg.stdout.decode('utf8'))
            except: # pylint: disable=bare-except # Never crash on this
                logging.exception('Could not check logs')

    else:
        logging.log(noerror_level, 'Worker %s exited with code %s',
                rpid, rret)

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
    logging.debug('Sending %s to all remaining %s children',
            sig, len(pids))
    for pid in pids:
        logging.debug('Sending %s to %s', sig, pid)
        os.kill(pid, sig)
        logging.debug('Sent %s to %s', sig, pid)
    for pid in pids:
        logging.debug('Waiting for %s', pid)
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
        logging.debug('Received signal %s', sig)
        if sig == signal.SIGCHLD:
            check_deaths(pids, sock_server)
        elif sig in (signal.SIGINT, signal.SIGTERM):
            kill_all(pids, sig)
            return True
        else:
            logging.debug('Ignoring signal %s', sig)
        return False
    selector.register(signal_read_fd, selectors.EVENT_READ, _on_signal)

# Listen socket and start workers
# ===============================

def on_socket_frames(frames, config, pids):
    """
    Handler for messages proxied to forkserver
    """
    msg = parse_core_message(frames)
    match msg:
        case Ok(gm.Fork() as fork):
            _config = dict(config, mission=fork.mission)
            pid = galp.worker.fork(_config)
            pids.add(pid)
        case _:
            logging.error('Unexpected %s', msg)

def register_socket_handler(selector, config, sock_server, pids):
    """
    Initialize the handler for broker messages, which does the actual forking
    work, and send the ready message
    """
    cpus = psutil.Process().cpu_affinity()
    if not cpus:
        raise RuntimeError('Could not read cpu affinity mask')
    socket_send_message(sock_server, gm.PoolReady(cpus=cpus))

    # Set up protocol
    multipart_reader = galp.socket_transport.make_multipart_generator(
            lambda frames: on_socket_frames(frames, config, pids)
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

# Forkserver IO loop
# ==================

def forkserver(sock_server, sock_logserver, signal_read_fd, config) -> None:
    """
    A dedicated loop to fork workers on demand and return the pids.

    Start this in a clean new thread that does not have a running asyncio event
    loop or any state of the sort.
    """
    selector = selectors.DefaultSelector()
    pids : set[int] = set()
    register_socket_handler(selector, config, sock_server, pids)
    register_signal_handler(selector, pids, signal_read_fd, sock_server)

    log_dir = os.path.join(config['store'], 'logs')
    os.makedirs(log_dir, exist_ok=True)
    galp.logserver.logserver_register(
            selector,
            sock_logserver, sock_server,
            log_dir)

    leave = False
    while not leave:
        events = selector.select()
        for key, _mask in events:
            callback = key.data
            leave = callback()


# Entry point
# ============

def main(config):
    """
    Main process entry point
    """
    galp.cli.setup(" pool ", config.get('log_level'))
    logging.info("Starting worker pool")

    # Socket pair forkserver <> proxy
    sock_server, sock_client = socket.socketpair()

    # Remember to close other socket ends
    # We don't use fork handlers as we're going to make many other unrelated
    # forks
    def _proxy_infork():
        sock_server.close()
        asyncio.run(proxy(config['endpoint'], sock_client))
    proxy_pid = galp.cli.run_in_fork(_proxy_infork)
    try:
        sock_client.close()

        # Socket pair forkserver <> workers
        sock_logserver, sock_logclient = socket.socketpair(
                socket.AF_UNIX, socket.SOCK_DGRAM
                )
        # Now we can put handlers for each new worker fork
        # The parent never closes, as it may need to fork again
        os.register_at_fork(after_in_child=sock_server.close)
        os.register_at_fork(after_in_child=sock_logserver.close)
        config['sock_logclient'] = sock_logclient

        # Self-pipe to receive signals
        signal_read_fd, signal_write_fd = os.pipe()
        os.set_blocking(signal_write_fd, False)
        signal.set_wakeup_fd(signal_write_fd, warn_on_full_buffer=True)
        for sig in (signal.SIGCHLD, signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: None)

        forkserver(sock_server, sock_logserver, signal_read_fd, config)

        logging.info("Pool manager exiting")
    finally:
        sock_server.close()
        os.waitpid(proxy_pid, 0)

def make_cli(config):
    """
    Generate a command line to spawn a pool
    """
    args = []
    missing = [key for key in ('endpoint', 'store') if key not in config]
    if missing:
        raise TypeError(f'Pool config missing required arguments: {missing}')
    for key, val in config.items():
        match key:
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
