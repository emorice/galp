"""
Dashboard functionality
"""

import pytest
import werkzeug
from bs4 import BeautifulSoup

import galp
import tests.steps as gts
from galp.dashboard import create_app

# pylint: disable=redefined-outer-name

@pytest.fixture
def render_parse(tmpdir, data):
    """
    Render a view in the dashboard as html and pass it through bs4
    """
    _ = data
    def _render_parse(name):
        url = f'/step/{name}'
        environ = werkzeug.test.create_environ(url)
        app = create_app(
            store=tmpdir,
            endpoints=gts.dashboard.get_endpoints(),
            )
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
            store=tmpdir)

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
    soup = render_parse('view_with_arg')
    assert any('42' in s for s in soup.strings)
