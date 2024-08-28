"""
Start and re-start processes, keep track of crashes and failed tasks
"""

import os
import sys
import asyncio
import logging
import time
import signal
import selectors
import subprocess
import socket

import psutil

import galp.worker
import galp.net.core.types as gm
import galp.socket_transport
import galp.logserver
from galp.result import Ok
from galp.socket_transport import AsyncClientTransport
from galp.protocol import make_stack
from galp.net.core.dump import dump_message
from galp.net.core.load import parse_core_message
from galp.async_utils import background

# Communication utils
# ===================

def socket_send_message(sock: socket.socket, message: gm.Message) -> None:
    """Serialize and send galp message over sock"""
    return galp.socket_transport.send_multipart(sock, dump_message(message))

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
    broker_transport = AsyncClientTransport(stack, broker_endpoint)

    async with background(
        broker_transport.listen_reply_loop()
        ):
        await listen_forkserver(forkserver_socket, broker_transport)

async def listen_forkserver(forkserver_socket, broker_transport) -> None:
    """Process messages from the forkserver"""
    forkserver_socket.setblocking(False)
    loop = asyncio.get_event_loop()
    multiparts: list[list[bytes]] = []
    receive = galp.socket_transport.make_receiver(multiparts.append)
    while True:
        buf = await loop.sock_recv(forkserver_socket, 4096)
        if not buf:
            return
        receive(buf)
        await broker_transport.send_messages([
            [b'', *frames]
            for frames in multiparts])
        multiparts.clear()

# Listen signals and monitor deaths
# =================================

def log_child_exit(rpid, rstatus,
                   noerror_level=logging.DEBUG) -> str:
    """
    Parse and log exit status.

    Returns:
        the log message, for additional consumers
    """
    rsig = rstatus & 0x7F
    rdumped = rstatus & 0x80
    rret = rstatus >> 8
    if rsig:
        if rsig in (signal.SIGTERM, signal.SIGINT):
            level = noerror_level
        else:
            level = logging.ERROR
        message = f'Worker {rpid} killed by signal {signal.Signals(rsig).name}'
        if rdumped:
            message += ' (core dumped)'
        logging.log(level, message)
        if rsig == signal.SIGKILL:
            try:
                # Check dmesg for OOM
                dmesg = subprocess.run(
                        f'dmesg -T | grep -i "Killed process {rpid}"',
                        shell=True, stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL, check=False)
                if dmesg.returncode == 0:
                    line = f'Worker {rpid} has a matching kernel log entry:'
                    logging.log(level, line)
                    message += ('\n' + line)
                    line = dmesg.stdout.decode('utf8')
                    logging.log(level, line)
                    message += ('\n' + line)
            except: # pylint: disable=bare-except # Never crash on this
                logging.exception('Could not check logs')

    else:
        message = f'Worker {rpid} exited with code {rret}'
        logging.log(noerror_level, message)

    return message

def check_deaths(children: dict[int, psutil.Process],
                 sock_server: socket.socket) -> None:
    """
    Check for children deaths and send messages to broker
    """
    rpid = -1
    while children and rpid:
        rpid, rstatus = os.waitpid(-1, os.WNOHANG)
        if rpid:
            if rpid not in children:
                logging.error('Ignoring exit of unknown child %s', rpid)
                continue
            error = log_child_exit(rpid, rstatus, logging.ERROR)
            # Before sending message, we should empty the std streams
            socket_send_message(sock_server, gm.Exited(str(rpid), error))
            del children[rpid]

def kill_all(children: dict[int, psutil.Process],
             sig: signal.Signals = signal.SIGTERM) -> None:
    """
    Kill all children and wait for them
    """
    logging.debug('Sending %s to all remaining %s children',
            sig, len(children))
    for pid in children:
        logging.debug('Sending %s to %s', sig, pid)
        os.kill(pid, sig)
        logging.debug('Sent %s to %s', sig, pid)
    for pid in children:
        logging.debug('Waiting for %s', pid)
        rpid = 0
        while not rpid:
            rpid, rexit = os.waitpid(pid, 0)
            log_child_exit(rpid, rexit)

def register_signal_handler(selector, children: dict[int, psutil.Process],
                            signal_read_fd, sock_server) -> None:
    """
    Set up signal handler to monitor worker deaths
    """
    def _on_signal():
        sig_b = os.read(signal_read_fd, 1)
        sig = signal.Signals(int.from_bytes(sig_b, 'little'))
        logging.debug('Received signal %s', sig)
        if sig == signal.SIGCHLD:
            check_deaths(children, sock_server)
        elif sig in (signal.SIGINT, signal.SIGTERM):
            kill_all(children, sig)
            return True
        else:
            logging.debug('Ignoring signal %s', sig)
        return False
    selector.register(signal_read_fd, selectors.EVENT_READ, _on_signal)

# Listen socket and start workers
# ===============================

def on_socket_frames(frames, config,
                     children: dict[int, psutil.Process]):
    """
    Handler for messages proxied to forkserver
    """
    msg = parse_core_message(frames)
    match msg:
        case Ok(gm.Fork() as fork):
            _config = dict(config, mission=fork.mission)
            pid = galp.worker.fork(_config)
            children[pid] = psutil.Process(pid)
        case _:
            logging.error('Unexpected %s', msg)

def register_socket_handler(selector, config, sock_server,
                            children: dict[int, psutil.Process]):
    """
    Initialize the handler for broker messages, which does the actual forking
    work, and send the ready message
    """
    cpus = psutil.Process().cpu_affinity()
    if not cpus:
        raise RuntimeError('Could not read cpu affinity mask')
    socket_send_message(sock_server, gm.PoolReady(cpus=cpus))

    # Set up protocol
    multipart_reader = galp.socket_transport.make_receiver(
            lambda frames: on_socket_frames(frames, config, children)
            )

    # Connect transport
    def _on_socket():
        buf = sock_server.recv(4096)
        if buf:
            multipart_reader(buf)
            return False
        # Disconnect
        return True
    selector.register(sock_server, selectors.EVENT_READ, _on_socket)

# Forkserver IO loop
# ==================

PSTAT_DELAY = 300.0 # Seconds

def hsize(nbytes: int) -> str:
    """
    Convert memory to human-readable string
    """
    fbytes = float(nbytes)
    for suffix in ('', 'Ki', 'Mi', 'Gi', 'Ti'):
        if fbytes < 1024:
            return f'{fbytes:.4g}{suffix}B'
        fbytes /= 1024
    # if somehow you got a machine with 2000TB of ram...
    return f'{fbytes*1024:.4g}{suffix}B'

def make_collect_stats(children: dict[int, psutil.Process]):
    """
    Create a hook to check and print child usage information

    This is implemented as a closure because it needs to keep a variable for the
    last measurement time. At each call, the function returns how much time is
    left til next measurement point.
    """

    last_time = time.time()

    def _collect_stats() -> float:
        nonlocal last_time
        cur_time = time.time()
        if (time_left := last_time + PSTAT_DELAY - cur_time) > 0:
            return time_left
        last_time = cur_time
        for process in children.values():
            with process.oneshot():
                cpu = process.cpu_percent()
                mem = process.memory_info()
            stats = f'{process.pid} {cpu:3.0f}% vms={hsize(mem.vms)} rss={hsize(mem.rss)}'
            print(stats, flush=True)
        return PSTAT_DELAY
    return _collect_stats


def forkserver(sock_server, sock_logserver, signal_read_fd, config) -> None:
    """
    A dedicated loop to fork workers on demand and return the pids.

    Start this in a clean new thread that does not have a running asyncio event
    loop or any state of the sort.
    """
    selector = selectors.DefaultSelector()
    children : dict[int, psutil.Process] = {}
    register_socket_handler(selector, config, sock_server, children)
    register_signal_handler(selector, children, signal_read_fd, sock_server)

    log_dir = os.path.join(config['store'], 'logs')
    os.makedirs(log_dir, exist_ok=True)
    galp.logserver.logserver_register(
            selector,
            sock_logserver, sock_server,
            log_dir)

    collect_stats = make_collect_stats(children)

    leave = False
    delay = PSTAT_DELAY
    while not leave:
        events = selector.select(delay)
        for key, _mask in events:
            callback = key.data
            leave = callback()
        delay = collect_stats()


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
