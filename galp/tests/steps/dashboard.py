"""
Dashboard specific steps, aka views
"""

import plotly.graph_objects as go

from galp import StepSet

export = StepSet()

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
