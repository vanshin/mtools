import json

from mtools.utils import valid
from mtools.resp.excepts import ParamError
from mtools.base.domain import Domain
from mtools.resp.define import CONF_OPEN


class Config(Domain):
    """系统使用的配置表 不同于KVConfig"""

    dbname = 'comistxs'
    table = 'kv_config'

    def __init__(self, app_code, app_secret=None):
        """后续需要使用秘钥才能解析配置"""
        super(Domain, self).__init__()
        self.app_secret = app_secret
        self.app_code = app_code

    def __getitem__(self, key):

        value_datas = self.gets(
            app_code=self.app_code, key=key,
            state=CONF_OPEN
        )

        if not value_datas:
            return None

        if len(value_datas) == 1:
            value_data = value_datas[0]

            if value_data['value_type'] == 'json':
                if valid.is_valid_loads(value_data['value']):
                    return json.loads(value_data['value'])
            if value_data['value_type'] == 'str':
                return value_data['value']
            if value_data['value_type'] == 'split_str':
                return value_data['value'].split(',')
            else:
                func = getattr(valid, f'is_valid_{value_data["value_type"]}', None)
                if not func:
                    raise ParamError('无法解析的配置类型')
                return func(value_data['value'])
        else:
            return [i['value'] for i in value_datas]



if __name__ == '__main__':
    conf = Config('tools')
    a = conf['test']
    print(a)
