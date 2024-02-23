import time, random, os, sys
import socket
import traceback
import logging
from mtools.server import selector
from mtools.base.httpclient import Urllib2Client

log = logging.getLogger()



class HttpClientError(Exception):
    pass

def with_http_retry(func):
    def _(self, *args, **kwargs):
        while True:
            try:
                result = func(self, *args, **kwargs)
                return result
            # 不要重试的错误
            except (HttpClientError, socket.timeout):
                if self.log_except:
                    log.warn(traceback.format_exc())
                if self.raise_except:
                    raise
                else:
                    return None
            # 重试的错误
            except:
                log.error(traceback.format_exc())
                if self.server:
                    self.server['valid'] = False
    return _

class HttpClient:
    def __init__(self, server, protocol='http', timeout=0, raise_except=False, log_except=True, client_class = Urllib2Client):
        self.server_selector  = None
        self.protocol = protocol
        self.timeout = timeout
        self.client_class = client_class
        self.client = None
        self.server = None
        self.raise_except = raise_except  # 是否在调用时抛出异常
        self.log_except = log_except  # 是否打日志

        if isinstance(server, dict): # 只有一个server
            self.server = [server,]
            self.server_selector = selector.Selector(self.server, 'random')
        elif isinstance(server, list): # server列表，需要创建selector，策略为随机
            self.server = server
            self.server_selector = selector.Selector(self.server, 'random')
        #elif isinstance(server, str) or isinstance(server, unicode):
        #    self.server = etcd_cache[server]
        else: # 直接是selector
            self.server_selector = server

        #如果无可用 尝试恢复
        if len(self.server_selector.valid()) == 0:
            http_restore(self.server_selector, self.protocol)

    @with_http_retry
    def call(self, func='get', path='/', *args, **kwargs):

        self.server = self.server_selector.next()
        if not self.server:
            raise HttpClientError('no valid server')

        domain = '%s://%s:%d' % (self.protocol, self.server['server']['addr'][0], self.server['server']['addr'][1])

        if self.timeout > 0:
            timeout = self.timeout
        else:
            timeout = self.server['server']['timeout']

        self.client = self.client_class(timeout = timeout/1000.0)

        func = getattr(self.client, func)
        return func(domain + path, *args, **kwargs)

    def __getattr__(self, func):
        def _(path, *args, **kwargs):
            return self.call(func, path, *args, **kwargs)
        return _

def http_restore(selector, protocol='http', path='/ping'):
    invalid = selector.not_valid()
    for server in invalid:
        try:
            log.debug('try restore %s', server['server']['addr'])
            domain = '%s://%s:%d' % (protocol, server['server']['addr'][0], server['server']['addr'][1])
            Urllib2Client(timeout=3).get(domain + path)
        except:
            log.error(traceback.format_exc())
            log.debug("restore fail: %s", server['server']['addr'])
            continue

        log.debug('restore ok %s', server['server']['addr'])
        server['valid'] = True


def test_http():
    from mtools.base import logger
    from mtools.base.httpclient import RequestsClient
    logger.install('stdout')
    SERVER   = [{'addr':('127.0.0.1', 6200), 'timeout':20},{'addr':('127.0.0.1', 6201), 'timeout':2000},]
    client = HttpClient(SERVER, client_class = RequestsClient)
    while 1:
        print(client.get('/ping'))
        raw_input('go')


def test_simple():
    from thriftclient3.payprocessor import PayProcessor
    from mtools.base import logger
    global log
    logger.install('stdout')
    log = logger.log
    log.debug('test ...')
    serverlist = [{'addr':('127.0.0.1',4300), 'timeout':1000},
                  {'addr':('127.0.0.1', 4200), 'timeout':1000},
                  ]
    sel = selector.Selector(serverlist)
    for i in range(0, 10):
        client = ThriftClient(sel, PayProcessor)
        client.ping()

    server = sel.next()
    server['valid'] = False

    #log.debug('restore ...')
    #restore(sel)
    print('-'*60)
    for i in range(0, 10):
        client = ThriftClient(sel, PayProcessor)
        client.ping()

def test_ping(port=1000):
    from thriftclient3.spring import Spring
    from mtools.base import logger
    global log
    log = logger.install('stdout')

    log.debug('test ...')
    serverlist = [
        {'addr':('127.0.0.1',port), 'timeout':1000},
        #{'addr':('127.0.0.1',4201), 'timeout':1000},
    ]
    sel = selector.Selector(serverlist)
    for i in range(0, 1000):
        client = ThriftClient(sel, Spring, framed=True)
        client.ping()


def test_selector():
    from thriftclient3.notifier import Notifier
    from mtools.base import logger
    global log
    logger.install('stdout')
    log.debug("test framed transport")
    serverlist = [
            {'addr':('172.100.101.151', 15555), 'timeout':1000},
            ]
    sel = selector.Selector(serverlist)
    client = ThriftClient(sel, Notifier, framed=True)
    notify = {
            "notify_url":"http://172.100.101.151:8989/",
            "notify_data": {
                    "orderstatus":"5",
                }
            }
    import json
    ret = client.send_notify(json.dumps(notify))
    log.debug("send notify return:%s", ret)

def test_name():
    from thriftclient3.payprocessor import PayProcessor
    from mtools.base import logger
    global log
    log = logger.install('stdout')
    log.debug('test ...')
    server_name = 'paycore'
    for i in range(0, 10):
        client = ThriftClient(server_name, PayProcessor)
        client.ping()


def test_perf():
    for i in range(0, 1000):
        test_ping(7200)

def test():
    f = globals()[sys.argv[1]]
    f()

if __name__ == '__main__':
    test()




