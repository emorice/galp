"""
Dashboard functionality
"""

import pytest
import werkzeug
from bs4 import BeautifulSoup

import galp
import galp.tests.steps as gts
from galp.graph import ensure_task
from galp.dashboard import create_app

# pylint: disable=redefined-outer-name

@pytest.fixture
def render_parse(tmpdir, data):
    """
    Render a view in the dashboard as html and pass it through bs4
    """
    _ = data
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

@pytest.fixture
def data(tmpdir):
    """
    Run some steps so that we have data to vizualize
    """
    galp.run(gts.dashboard.fortytwo,
            store=tmpdir,
            steps=['galp.tests.steps']
            )

def test_plotly(render_parse):
    """
    Render plotly figures to html in views
    """
    soup = render_parse('plotly_figure')
    assert len(soup.select('.plotly-graph-div')) == 1

def test_inject(render_parse):
    """
    Render a view depending on data in store
    """
    soup = render_parse('view_with_inject')
    assert any('42' in s for s in soup.strings)

@pytest.mark.xfail
def test_inject_literal(render_parse):
    """
    Render a view depending on a graph constant
    """
    soup = render_parse('view_with_injected_literal')
    assert any('42' in s for s in soup.strings)
