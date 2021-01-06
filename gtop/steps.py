"""
Steps of the main pipelines
"""

import galp.graph

export = galp.graph.StepSet()

@export.step
def gtex_gt_paths():
    """The paths of GTEx genotypes files, acording to local config"""
    pass

@export.step
def file_sizes(paths):
    """Stats the given path and report their sizes."""
    pass
