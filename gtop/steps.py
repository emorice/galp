"""
Steps of the main pipelines
"""

import os
import time
import gzip
import re
import warnings
from ctypes import POINTER, c_double
from llvmlite import ir

import numpy as np
import tables

import galp.graph

# Todo: should be moved to an injectable at some point
import local.config as config

from gtop.parsing.vcf import vcf_calls_auto
from gtop.parsing.autocompile import a_to_function

export = galp.graph.StepSet()

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)
    return path

@export.step()
def extra_data_dir():
    """Where to store files outside of the cache system

    The cache is the privileged place where to store data, so this should be
    solely for data that needs to be accessed from outside the pipeline,
    typically for testing.
    """
    return ensure_dir(config.EXT_DATA_DIR)

@export.step()
def safe_data_dir():
    """Where to store sensitive files outside of the cache system."""
    return ensure_dir(config.CRYPT_EXT_DATA_DIR)

@export.step
def gtex_gt_paths():
    """The paths of GTEx genotypes files, acording to local config"""
    return [config.GTEX_PATH]

@export.step(vtag='fiximport')
def file_sizes(paths):
    """Stats the given path and report their sizes."""
    return {
        'files': [ {
            'size': os.stat(path).st_size
            } for path in paths ]
        }

@export.step(vtag='blosc9')
def vcf_to_hdf(paths, out_dir, batch_size, max_batches=0):
    """Simple GT parser from vcf.gz that loads into an hdf file.

    Args:
        paths: list of paths to files to process, one output file will be
            created for each.
        out_dir: Path to directory where to place resulting files. Beware of
            keeping the same level of storage security.
        batch_size: how many lines to process at once.
        max_batches: if non-zero, stop after processing at most this number of
            batches, useful to test on a part of a large file
    TODO: not hardened against badly-constructed files
    """
    out_paths = { 'files': [] }
    for path in paths:
        ts_start = time.time()
        with gzip.open(path) as in_fd:
            lines = iter(in_fd)

            # Header
            header = next(line for line in lines if line.startswith(b'#CHROM'))
            columns = header.split(b'\t')
            n_fixed_columns = columns.index(b'FORMAT') + 1
            n_samples = len(columns) - n_fixed_columns

            # Output file
            hdf_path = os.path.join(out_dir, str(ts_start) + '.h5')
            out_fd = tables.open_file(hdf_path, mode='w')
            out_array = out_fd.create_earray('/', 'gt',
                shape=(0, n_samples),
                atom=tables.FloatAtom(), 
                filters=tables.Filters(complevel=9, complib='blosc')
                )

            # Prepare stream conversion
            auto = vcf_calls_auto(n_fixed_columns)
            filt = a_to_function(auto, endchar=b'\n',
                out_t=ir.DoubleType(), out_ctype=POINTER(c_double))

            # Actual batch processing
            variants = 0
            batches = 0
            while True and (batches < max_batches or not max_batches):
                batch = np.empty((batch_size, 2*n_samples), np.float64)
                assert all((
                    batch.flags.c_contiguous,
                    batch.flags.owndata,
                    batch.flags.writeable,
                    batch.flags.aligned
                    ))
                read = 0
                for _, line in zip(range(batch_size), lines):
                    row = batch[read, :]
                    assert all((
                        row.flags.c_contiguous,
                        not row.flags.owndata,
                        row.flags.writeable,
                        row.flags.aligned
                        ))
                    n_values = filt(line, row.ctypes.data_as(POINTER(c_double)))
                    if not n_values in (0, 2 * n_samples):
                        raise ValueError(n_values)
                    if n_values:
                        read += 1
                if not read:
                    break
                batch = batch[:read, :]

                dosage = batch[:,::2] + batch[:,1::2]

                out_array.append(dosage)
                variants += dosage.shape[0]
                batches += 1

            out_fd.close()

            details = {
                'path': hdf_path,
                'variants': variants,
                'samples': n_samples,
                'compressed_size': in_fd.fileobj.tell(),
                'decompressed_size': in_fd.tell(),
                'batches': batches,
                'wall_time_s': time.time() - ts_start
            }
            details['compression_ratio'] = (details['decompressed_size'] / 
                details['compressed_size'])

            out_paths['files'].append(details)
    return out_paths

