'''RPC Api Handler for DevOps Projects -- fanxin-group'''

import logging

from mtools.domain import Domain, drive
from mtools.server.rpcserver import Handler

from mtools.base.builder import builder_map

log = logging.getLogger()


__version__ = '1.0.0'


class RPCHandler(Handler):
    '''BuildHandler是处理请求的工具集合Handler

    1. 包含builder中的所有的工具，工具按照插件的概念
       使用时加载，并且为工具提供本次请求的数据和状态
    ArgsBuilder: 处理请求的的参数，用来验证数据类型和业务类型等
    ListBuilder: 用来处理分页请求数据
    ExpoBuilder: 用来处理导出到文件的处理

    '''
    def __init__(self, *args, **kw):
        super(RPCHandler, self).__init__(*args, **kw)
        self.resp.extend = self.resp.extend or {}

        # 设置builder
        self._builder_map = builder_map
        self.params = self.data.params

        # 加载本次请求的信息
        ses = {}
        self.userid = None
        if self.data.extend and 'session' in self.data.extend:
            ses = self.data.extend['session'] or {}
            self.userid = ses.get('userid')

        self._ses_fill = False

        self.source = {'args': self.data.params, 'ses': ses, 'method': 'rpc'}

    def dativer(self, *args, **kw):
        pass

    def rcall(self, rpc, method, params):
        return Domain().rcall(rpc, method, params)

    def drive(self, method, data):
        return drive(method, data)

    def __getattr__(self, name):
        '''加载builder工具'''

        if self._builder_map and name in self._builder_map:

            args = self.params
            self.source['args'] = args

            if not self._ses_fill and getattr(self, 'ses', None):
                self._ses_fill = True
                self.source['ses'] = self.ses.data

            # 实例化builder
            builder_cls = self._builder_map[name]
            builder = builder_cls(self.source)
            setattr(self, builder.name, builder)
            return builder

        raise AttributeError

    def class_name(self):
        return self.__class__.__name__

    def run_builder(self, builder_cls, *arg, **kw):
        args = self.params
        self.source['args'] = args
        if not self._ses_fill and getattr(self, 'ses', None):
            self._ses_fill = True
            self.source['ses'] = self.ses.data
        builder = builder_cls(self.source)
        builder.init(*arg, **kw)
        return builder.run()
