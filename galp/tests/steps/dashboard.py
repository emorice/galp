"""
Dashboard specific steps, aka views
"""

import plotly.graph_objects as go

import galp

export = galp.Block()

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

@export.view
def view_with_injected_literal(bound_fortytwo):
    """
    View requiring a bound_constant
    """
    return {'data': bound_fortytwo}

export.bind(bound_fortytwo={'x': fortytwo})
