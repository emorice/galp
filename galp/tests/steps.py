"""
Steps only used for testing, and loaded through the plugin system.
"""

from galp.graph import StepSet

export = StepSet()

@export.step
def plugin_hello():
    return '6*7'
