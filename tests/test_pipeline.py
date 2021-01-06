"""
All kinds of tests related to the main pipelines.
"""

import asyncio
import pytest
import logging
import zmq
import zmq.asyncio

import galp.client
from gtop.steps import file_sizes, gtex_gt_paths

# Fixtures
# ========

@pytest.fixture
def med_endpoint():
    # Where to contact a worker with med access
    return 'ipc://var/worker.med.sock'

@pytest.fixture
def med_client(med_endpoint):
    # A client connected with a worker granted access to medical data
    return galp.client.Client(endpoint=med_endpoint)

# Tests
# =====

@pytest.mark.xfail
@pytest.mark.asyncio
async def test_gtex_size(med_client):
    """Test that we can access the GTEx files"""
    
    task = file_sizes(gtex_gt_paths())

    sizes = await asyncio.wait_for(med_client.collect(task), 3)

    assert 'files' in sizes
    assert len(sizes['files']) == 23
    assert all(s['size'] == int(s['size']) and s['size'] > 0 for s in sizes['files'])
    logging.warning('Total size %d', sum(s['sizes'] for s in sizes['files']))



