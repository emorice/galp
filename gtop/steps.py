"""
Steps of the main pipelines
"""

import os
import galp.graph

# Todo: should be moved to an injectable at some point
import local.config as config

export = galp.graph.StepSet()

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
