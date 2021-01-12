"""
Pipeline steps defined for testing purposes
"""

import galp.graph

export = galp.graph.StepSet()

@export.step
def test_vcf_paths():
    return ['tests/assets/sample.vcf.gz']
