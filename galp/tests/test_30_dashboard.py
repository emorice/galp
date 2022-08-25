"""
Dashboard functionality
"""

import pytest
import werkzeug
from bs4 import BeautifulSoup

import galp.tests.steps as gts
from galp.graph import ensure_task
from galp.dashboard import create_app

# pylint: disable=redefined-outer-name

@pytest.fixture
def render_parse(tmpdir):
    """
    Render a view in the dashboard as html and pass it through bs4
    """
    def _render_parse(name):
        url = f'/step/galp.tests.steps.dashboard::{name}'
        environ = werkzeug.test.create_environ(url)
        app = create_app({
            'log_level': 'info',
            'store': tmpdir,
            'steps': ['galp.tests.steps']
            })
        ctx = app.request_context(environ)
        ctx.push()
        try:
            resp = app.dispatch_request()
        finally:
            ctx.pop()
        return BeautifulSoup(resp, features="html.parser")
    return _render_parse

def test_plotly(render_parse):
    """
    Render plotly figures to html in views
    """
    soup = render_parse('plotly_figure')
    assert len(soup.select('.plotly-graph-div')) == 1
