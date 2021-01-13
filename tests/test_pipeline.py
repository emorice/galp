"""
All kinds of tests related to the main pipelines.
"""

import os
import sys
import asyncio
import subprocess
import logging
import json

import pytest

import zmq
import zmq.asyncio

import numpy as np
import tables

import galp.client
import gtop.steps
# Often used steps
from gtop.steps import file_sizes, gtex_gt_paths, extra_data_dir, safe_data_dir
import tests.steps

# Fixtures
# ========

@pytest.fixture
def med_endpoint():
    # Where to contact a worker with med access
    return 'ipc://var/worker.med.sock'

@pytest.fixture
def local_endpoint():
    # Where to contact a worker running on the local machine
    return 'ipc://var/worker.local.sock'

@pytest.fixture
def local_client(local_endpoint, tmp_path):
    """A client, and worker, running on the local machine.

    Only a temporary cache.
    """
    phandle = subprocess.Popen([
        sys.executable,
        '-m', 'galp.worker',
        '-c', 'tests/config/worker.toml',
        local_endpoint, str(tmp_path)
        ])

    yield galp.client.Client(endpoint=local_endpoint)

    phandle.terminate()
    phandle.wait()

@pytest.fixture
def med_client(med_endpoint):
    """A client connected with a worker granted access to medical data"""
    return galp.client.Client(endpoint=med_endpoint)

@pytest.fixture
def sample_vcf_path():
    return 'tests/assets/sample.vcf.gz'

@pytest.fixture
def expected_gt():
    """A numpy array of the expected content of the assets/sample.vcf.gz file"""
    return np.array([[1., 1.],
       [1., 2.],
       [np.nan, 0.],
       [1., 2.]])

# Tests
# =====

@pytest.mark.asyncio
async def test_gtex_size(med_client):
    """Test that we can access the GTEx files"""
    
    task = file_sizes(gtex_gt_paths())

    ans = await asyncio.wait_for(med_client.collect(task), 3)
    sizes = ans[0]

    assert 'files' in sizes
    assert len(sizes['files']) == 1 # Gtex has one big genome file
    assert all(s['size'] == int(s['size']) and s['size'] > 0 for s in sizes['files'])
    total = sum(s['size'] for s in sizes['files'])
    logging.warning('Total size %d GiB (%d)', total // (2**30), total)


@pytest.mark.asyncio
async def test_vcf_to_hdf(local_client, sample_vcf_path, expected_gt):
    """Test streaming extraction of data from possibly large vcf file to hdf"""

    task = gtop.steps.vcf_to_hdf([sample_vcf_path],
        extra_data_dir(), batch_size=2)

    ans = (await asyncio.wait_for(local_client.collect(task), 2))[0]
    assert 'files' in ans
    assert len(ans['files']) == 1

    details = ans['files'][0]
    hdf_path = details['path']

    assert os.path.isfile(hdf_path)
    assert details['samples'] == 2
    assert details['variants'] == 4
    assert details['compressed_size'] == os.stat(sample_vcf_path).st_size
    assert details['decompressed_size'] > details['compressed_size']
    assert details['compression_ratio'] > 1.0
    assert details['wall_time_s'] > 0
    logging.warning('VCF details: %s', json.dumps(details, indent=1))

    fd = tables.open_file(hdf_path)

    data = fd.root.gt.read()

    np.testing.assert_array_equal(data, expected_gt)

    fd.close()

@pytest.mark.asyncio
async def test_gtex_gt_hdf(med_client):
    """
    Tests converting the beginning og GTEx GT to hdf and informally benchmark
    """

    task = gtop.steps.vcf_to_hdf(gtex_gt_paths(), safe_data_dir(),
        batch_size=1000, max_batches=10)

    details = (await asyncio.wait_for(med_client.collect(task), 60))[0]['files'][0]

    logging.warning('VCF details: %s', json.dumps(details, indent=1))

    assert details['variants'] == 10000
