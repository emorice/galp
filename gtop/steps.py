"""
Steps of the main pipelines
"""

import os
import time
import gzip
import re
import warnings

import numpy as np
import tables

import galp.graph

# Todo: should be moved to an injectable at some point
import local.config as config

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

@export.step
def vcf_to_hdf(paths, out_dir):
    """Simple GT parser from vcf.gz that loads into an hdf file.

    TODO: not hardened against badly-constructed files
    """
    out_paths = { 'files': [] }
    for path in paths:
        with gzip.open(path) as in_fd:
            lines = iter(in_fd)

            # Header
            header = next(line for line in lines if line.startswith(b'#CHROM'))
            columns = header.split(b'\t')
            n_fixed_columns = columns.index(b'FORMAT') + 1
            n_samples = len(columns) - n_fixed_columns

            # Output file
            ts = time.time()
            hdf_path = os.path.join(out_dir, str(ts) + '.h5')
            out_fd = tables.open_file(hdf_path, mode='w')
            out_array = out_fd.create_earray('/', 'gt',
                shape=(0, n_samples),
                atom=tables.FloatAtom(), 
                filters=tables.Filters(complevel=9)
                )

            # Prepare stream conversion
            non_data_pattern = re.compile(rb'^' + rb'[^\t]*\t' * n_fixed_columns) 
            data_columns = (non_data_pattern.sub(b'', line) for line in lines)

            gt_pattern = re.compile(rb'([^:\t])(\||/)([^:\t]):?[^\t]*(\t|$)')
            gt_tsv = (gt_pattern.sub(rb'\1\t\3\4', line) for line in data_columns)

            # Actual batch processing
            batch_size = config.vcf_batch_size
            while True:
                tsv_batch = (line for _, line in zip(range(batch_size), gt_tsv))
                # There is no easy way to check if a batch is empty without a
                # copy, so just let numpy deal with it
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', r'loadtxt: Empty input file')
                    batch = np.loadtxt(tsv_batch, delimiter='\t', ndmin=2)
                if not batch.size:
                    break

                dosage = batch[:,::2] + batch[:,1::2]

                out_array.append(dosage)

            out_fd.close()

            out_paths['files'].append({'path': hdf_path})
    return out_paths

