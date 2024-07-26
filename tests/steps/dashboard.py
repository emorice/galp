"""
Dashboard specific steps, aka views
"""

from typing import Any

import plotly.graph_objects as go # type: ignore[import]

import galp

# pylint: disable=redefined-outer-name

@galp.view
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

@galp.step
def fortytwo():
    """
    Constant step
    """
    return 42

@galp.view
def view_with_arg(value):
    """
    View requiring the output of an other step
    """
    return {'data': value}

def get_endpoints() -> dict[str, Any]:
    """
    Dict of items to include in dashboard
    """
    return {
            'plotly_figure': plotly_figure(),
            'view_with_arg': view_with_arg(fortytwo())
            }
