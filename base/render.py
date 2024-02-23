from mtools.web import template
# from common.base import BuildHandler
from mtools.web.core import Handler


class FrontProxyRender(Handler):
    """适用于vue的前端代理"""

    index_path = ''
    index_name = 'index.html'

    def GET(self, *args, **kw):

        r = template.Render(self.index_path)
        return self.write(r.display(self.index_name))


def GET(self, *args, **kw):

    r = template.Render(self.index_path)
    return self.write(r.display(self.index_html))


def render_factory(app_code, index_path, index_html='index.html'):

    return type(
        f'{app_code.capitalize()}FrontProxyRender',
        (Handler,),
        {'GET': GET, 'index_path': index_path, 'index_html': index_html}
    )
