"""
Dashboard specific steps, aka views
"""

import plotly.graph_objects as go

from galp import StepSet

export = StepSet()

# pylint: disable=redefined-outer-name

@export.view
def plotly_figure():
    """
    View that returns a plotly figure object
    """
    return go.Figure(
            go.Scatter(
                x=[1, 2, 3, 4],
                y=[9, 7, 8, 6]
                )
            )

@export
def fortytwo():
    """
    Constant step
    """
    return 42

@export.view
def view_with_inject(fortytwo):
    """
    View requiring the output of an other step
    """
    return {'data': fortytwo}
