"""
Fixtures related to creating galp processes to interact with
"""
import itertools
import subprocess
import sys

import pytest

@pytest.fixture
def port():
    # TODO: these ports could be in use
    ports = itertools.count(48652)

    def _next_port(ports=ports):
        return next(ports)

    return _next_port

@pytest.fixture
def make_process():
    """Worker fixture, starts a worker pool in background.

    Returns:
        (endpoint, Popen) tuple
    """
    phandles = []

    def _make(*arg_list):
        phandle = subprocess.Popen([
            sys.executable,
            *arg_list
            ])
        phandles.append(phandle)

        return phandle

    yield _make

    for phandle in phandles:
        try:
            phandle.terminate()
            phandle.wait()
        except ProcessLookupError:
            pass


@pytest.fixture
def make_worker(make_process, port, tmp_path):
    """Worker fixture, starts a worker in background.

    Returns:
        (endpoint, Popen) tuple
    """
    def _make(endpoint=None):
        if endpoint is None:
            endpoint = f"tcp://127.0.0.1:{port()}"

        phandle = make_process(
            '-m', 'galp.worker',
            '-c', 'galp/tests/config.toml',
            #'--debug',
            endpoint, str(tmp_path)
            )

        return endpoint, phandle

    return _make

@pytest.fixture
def make_pool(make_process, port, tmp_path):
    """Pool fixture, starts a worker pool in background.

    Returns:
        (endpoint, Popen) tuple
    """
    def _make(endpoint=None, pool_size=1):
        if endpoint is None:
            endpoint = f"tcp://127.0.0.1:{port()}"

        phandle = make_process(
            '-m', 'galp.pool',
            str(pool_size),
            '-c', 'galp/tests/config.toml',
            #'--debug',
            endpoint, str(tmp_path)
            )

        return endpoint, phandle

    return _make

@pytest.fixture
def make_broker(make_process, port, tmp_path):
    """Broker fixture, starts a broker in background.

    Returns:
        (client_endpoint, Popen) tuple
    """
    def _make():
        client_endpoint = f"tcp://127.0.0.1:{port()}"
        worker_endpoint = f"tcp://127.0.0.1:{port()}"

        phandle = make_process(
            '-m', 'galp.broker',
            #'--debug',
            client_endpoint,
            worker_endpoint,
            )
        return client_endpoint, worker_endpoint, phandle

    return _make

@pytest.fixture
def make_galp_set(make_broker, make_pool):
    """
    A set of one broker, one pool manager and n connected workers

    Returns:
        (client_endpoint, (broker_handle, pool_handle))
    """
    def _make(n):
        cl_ep, w_ep, broker_handle = make_broker()
        w_ep2, pool_handle = make_pool(w_ep, n)
        assert w_ep == w_ep2
        return cl_ep, (broker_handle, pool_handle)

    return _make

@pytest.fixture
def worker(make_worker):
    """
    A standalone worker
    """
    return make_worker()

@pytest.fixture
def galp_set_one(make_galp_set):
    """
    A set including a single worker
    """
    return make_galp_set(1)

@pytest.fixture
def galp_set_many(make_galp_set):
    """
    A set including 10 workers
    """
    return make_galp_set(10)
