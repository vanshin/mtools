'''Dativer over RPC Api Handler'''
import copy
import json
import networkx as nx
from collections import defaultdict

import logging

from mtools.base.handler.rpc import RPCHandler
from mtools.base.builder import DataBuilder
from mtools.base.field import Str, List, Dict
from mtools.base.dbpool import get_connection
from mtools.resp.excepts import ParamError

from mtools.utils.strutils import dict_str, list_str


log = logging.getLogger()


__version__ = '1.0.0'


class BaseData(DataBuilder):

    @Str(must=True)
    def object(self, value):
        '''可判断字段是否都存在'''
        return value

    @Str(must=True)
    def namespace(self, value):
        '''可判断字段是否都存在'''
        return value

    @Dict(must=False)
    def setting(self, value):
        '''可判断字段是否都存在'''
        return value or {}


class QueryData(BaseData):

    @Dict(must=True)
    def rule(self, value):
        '''可判断字段是否都存在'''
        return value

    @List(must=True)
    def field(self, value):
        '''可判断字段是否都存在'''
        return value


class UpdateData(BaseData):

    @Dict(must=True)
    def data(self, value):
        '''可判断字段是否都存在'''
        return value


class CreateData(BaseData):

    def data(self, value):
        '''可判断字段是否都存在'''
        if isinstance(value, dict):
            return [value]
        elif isinstance(value, list):
            return value
        else:
            raise ParamError('非法的类型')


class MetaData(BaseData):

    @Str(must=False)
    def object(self, value):
        '''可判断字段是否都存在'''
        return value


class FieldFunc(object):

    def fen2yuan(self, v):
        return v / 100, 2

    def split(self, v):
        return v.split(',')

    def loads(self, v):
        log.debug(v)
        return json.loads(v)

    def cap(self, v):
        return str(v).capitalize()

    def default(self, v):
        return v


class Dativer(RPCHandler):
    '''
    提供数据支持
    '''

    user = {
        '_name': 'user',
        '_name_descr': '基础用户表',
        'id': 'int',
    }

    user_role_bind = {

        '_name': 'user_role_bind',
        'id': 'int',
        'user_id': {'type': 'int', 'relate': 'user.id'},
        'role_id': {'type': 'int', 'relate': 'role.id'},
    }

    role = {
        '_name': 'role',
        'id': 'int',
    }

    perm = {
        '_name': 'perm',
        'id': 'int',
    }

    role_perm_bind = {
        '_name': 'role_perm_bind',
        'id': 'int',
        'perm_id': {'type': 'int', 'relate': 'perm.id'},
        'role_id': {'type': 'int', 'relate': 'role.id'},
    }

    schemas = [user, role, perm, user_role_bind, role_perm_bind]

    attr_op_map = {
        'ge': '>=',
        'gt': '>',
        'lt': '<',
        'le': '<=',
        'neq': '!='
    }

    magic_empty_list = [-28745]
    magic_empty = -28745

    default_object = '_default_object'
    default_func = '_default_func'

    def __new__(cls, *args, **kw):
        instance = super().__new__(cls)

        graph = nx.Graph()
        for i in cls.schemas:
            db_name = i['_name']
            graph.add_node(db_name)
            for k, v in i.items():
                if not isinstance(v, dict):
                    continue
                if 'relate' not in v:
                    continue
                relations = v['relate']
                if not isinstance(v['relate'], list):
                    relations = [v['relate']]
                for i in relations:
                    linked_db_name = i.split('.')[0]
                    # 添加数据库关系
                    graph.add_edge(db_name, linked_db_name)
                    # 添加数据库关系属性
                    # {'adb': 'bdb_id', 'bdb': 'id'}
                    graph.edges[db_name, linked_db_name]['relation'] = {
                        db_name: f'{db_name}.{k}',
                        linked_db_name: i
                    }
        cls.graph = graph
        cls.fielder = FieldFunc()
        return instance

    def __init__(self, *args, **kw):
        super(Dativer, self).__init__(*args, **kw)
        self._query_result_cache = {}
        self._load_schemas()

    def _load_schemas(self):
        self.name_schema = {}
        for i in self.schemas:
            self.name_schema[i['_name']] = i

    def _escape_fuzzy(self, value, rule_func):

        for i in ['%', '_']:
            if i in value:
                value = value.replace(i, '\\' + i)
        if rule_func == 'fuzzy':
            return f'%{value}%'
        elif rule_func == 'lfuzzy':
            return f'%{value}'
        elif rule_func == 'rfuzzy':
            return f'{value}%'
        elif rule_func == 'and_fuzzy':
            return [f'%{i.strip()}%' for i in value.split('|')]

    def _query_rule_func(self, rule_func, value):
        """处理前缀函数"""

        func_value = value
        if rule_func == 'default':
            if isinstance(value, list):
                func_value = ('in', value)
        # rule_func fuzzy
        elif 'fuzzy' in rule_func:
            func_value = self._escape_fuzzy(value, rule_func)
            func_value = ('like', func_value)
        # rule_func timein
        elif rule_func == 'btw':
            func_value = ('between', value)
        # rule_func other
        elif rule_func in self.attr_op_map:
            func_value = (self.attr_op_map[rule_func], value)

        return func_value

    def _query_field_func(self, func, values):
        log.debug(func)
        func = getattr(self.fielder, func, self.fielder.default)
        log.debug(func)
        if not isinstance(values, list):
            return func(values)
        else:
            return list(map(func, values))

    def _parse_item(self, item):
        """解析 lfuzzy.role__role_name"""

        item_func = self.default_func
        item_object = self.default_object

        if '.' in item:
            item_func, item = item.split('.')
        if '__' in item:
            item_object, item = item.split('__')
        return item_func, item_object, item

    def _option_setting(self, option):
        setting = option['setting']
        self.setting = setting
        self.pagination = False
        self.select_one = False
        if 'pagination' in self.setting:
            self.pagination = True
        elif 'one' in self.setting:
            self.select_one = True

        self.after = self.setting.get('after') or []
        self.before = self.setting.get('before') or []

    def _query_cache(self, db_name, table1, table2, on_dict, fields, where):
        """内存缓存，缓存只在实例生命周期生效"""

        _key = f'{db_name}:{table1}:{table2}:{dict_str(on_dict)}:{list_str(fields)}:{dict_str(where)}'
        if _key in self._query_result_cache:
            return self._query_result_cache[_key]

        with get_connection(db_name) as db:
            records = db.select_join(
                table1 = table1,
                table2 = table2,
                on = on_dict,
                fields = fields,
                where = where
            )

        self._query_result_cache[_key] = records

        return records

    def rule_query(self, option, list_conf, list_conf_data):
        """处理rule规则"""

        back_query = defaultdict(dict)

        for k, v in option['rule'].items():
            rule_func, rule_obj, rule_item = self._parse_item(k)

            # 主对象的字段查询直接使用
            if rule_obj == self.default_object:
                if rule_func == self.default_func:
                    list_conf['rules'].append(rule_item)
                else:
                    list_conf['rules'].append(f'{rule_func}.{rule_item}')
                list_conf_data[rule_item] = v
                continue

            # 非主对象的查询转到主对象属性中
            source_target = f'{rule_obj}:{option["object"]}'
            db_rule_item = f'{rule_obj}.{rule_item}'

            # 解析查询规则方法
            v = self._query_rule_func(rule_func, v)

            # 多表同一字段合并
            if source_target in back_query:
                back_query[source_target]['query'][db_rule_item] = v
                continue

            # back_query dict query:query params short_path: oa->ob path
            # not support same db_rule_item with diff rule value
            sp = nx.shortest_path(self.graph, rule_obj, option['object'])
            back_query[source_target]['query'] = {db_rule_item: v}
            back_query[source_target]['short_path'] = sp

        for source_target, query_data in back_query.items():

            short_path = query_data['short_path']

            idx = 0
            last = len(short_path) - 1
            query = query_data['query']

            while idx < last:
                f_db = short_path[idx]
                b_db = short_path[idx+1]
                relation = self.graph.edges[f_db, b_db]['relation']

                # left 1 distance
                if (last - idx) == 1:
                    field = relation[b_db]
                # more than 1 distance
                else:
                    nxt_f_db = short_path[idx+2]
                    field = self.graph.edges[b_db, nxt_f_db]['relation'][b_db]

                # two table join query
                _field = f'{field} as `{field}`'
                records = self._query_cache(
                    option['namespace'], f_db, b_db, {relation[f_db]: relation[b_db]},
                    _field, query
                )

                query_values = [i[field] for i in records] or self.magic_empty_list
                query = {field: ('in', query_values)}
                idx += 1

            # finish one back_query
            field_item = field.split('.')[1]
            if field_item not in list_conf_data:
                list_conf_data[field_item] = query_values
                list_conf['rules'].append(field_item)
            else:
                field_item_data = set(list_conf_data[field_item])
                field_item_data = field_item_data.intersection(query_values)
                list_conf_data[field_item] = list(field_item_data) or self.magic_empty_list
        return list_conf, list_conf_data

    def build_list(self, list_conf, list_conf_data):
        """主对象查询"""
        _how = 'data'
        if self.pagination == True:
            list_conf_data['_page'] = self.setting['page']
            list_conf_data['_size'] = self.setting['size']
            _how = 'query'
        list_conf['fields'] = list(set(list_conf['fields']))
        self.r = self.listblr.build(list_conf, list_conf_data, how=_how)
        result_data = self.r
        if _how == 'query':
            result_data = self.r['list']
        return result_data

    def field_query(self, option, list_conf, list_conf_data):

        front_query = defaultdict(dict)
        for i in option['field']:

            field_func, field_obj, field_item = self._parse_item(i)

            if field_obj == self.default_object:
                list_conf['fields'].append(field_item)
                continue

            source_target = f'{option["object"]}:{field_obj}:{field_item}'
            sp = nx.shortest_path(self.graph, option['object'], field_obj)

            relation = self.graph.edges[sp[0], sp[1]]['relation']
            base_over_key = relation[sp[0]]
            list_conf['fields'].append(base_over_key.split('.')[1])
            spec_key = f'{field_obj}.{field_item}'

            front_query[source_target]['spec_key'] = spec_key
            front_query[source_target]['spec_key_func'] = field_func
            front_query[source_target]['short_path'] = sp
            front_query[source_target]['base_over_key'] = base_over_key
        #
        result_data = self.build_list(list_conf, list_conf_data)

        for source_target, query_data in front_query.items():

            short_path = query_data['short_path']
            spec_key = query_data['spec_key']
            spec_key_func = query_data['spec_key_func']
            base_over_key = query_data['base_over_key']
            idx = 0
            last = len(short_path) - 1
            _base_over_key = base_over_key.split('.')[1]
            log.debug(spec_key_func)

            # 第一次查询的查询值
            query_over_key = base_over_key
            _query_over_key = query_over_key.split('.')[1]
            over_value = {
                query_over_key: ('in', [i[_query_over_key] for i in result_data] or self.magic_empty_list)
            }
            # 查询结果
            over_query_map = {}


            # 开始查询
            while idx < last:
                f_db = short_path[idx]
                b_db = short_path[idx+1]

                # 两表关联关系
                relation = self.graph.edges[f_db, b_db]['relation']
                query_over_key = relation[f_db]
                # 最后一次查询指定的key
                if (last - idx) == 1:
                    _field = spec_key
                # 还在中间
                else:
                    nxt_f_db = short_path[idx+2]
                    nxt_relation = self.graph.edges[b_db, nxt_f_db]['relation']
                    _field = nxt_relation[b_db]
                fields = [f'{_field} as `{_field}`']
                fields.append(f'{query_over_key} as `{query_over_key}`')
                #

                records = self._query_cache(
                    option['namespace'], f_db, b_db,
                    {relation[f_db]: relation[b_db]}, fields, over_value
                )
                # 下次查询值
                _over_value = [i[_field] for i in records] or self.magic_empty_list
                over_value = {_field: ('in', _over_value)}

                # merge
                _over_query_map = defaultdict(list)
                for i in records:
                    _over_query_map[i[query_over_key]].append(i[_field])

                # 第一次
                if not over_query_map:
                    over_query_map = _over_query_map
                else:
                    for k, v in over_query_map.items():
                        new_v = []
                        for i in v:
                            new_v.extend(_over_query_map[i])
                        over_query_map[k] = new_v

                # 索引前进
                idx += 1

            # 合并数据
            _result_data = {i[_base_over_key]: i for i in result_data}
            for base_over_value, data in _result_data.items():
                _spec_key = spec_key.replace('.', '__')
                spec_key_values = over_query_map[base_over_value]
                # spec_key_values = self._query_field_func(spec_key_func, spec_key_values)
                data[_spec_key] = spec_key_values[0] if spec_key_values else spec_key_values
            result_data = list(_result_data.values())

        return result_data


    def query(self, *args, **kw):

        # 参数验证
        option = self.run_builder(QueryData)

        # 解析用户选项
        self._option_setting(option)

        list_conf = {
            'source': option['namespace'] + '.' + option['object'],
            'fields': [],
            'rules': [],
        }
        list_conf_data = {}

        # 加载查询规则
        list_conf, list_conf_data = self.rule_query(option, list_conf, list_conf_data)
        # 加载查询字段
        result_data = self.field_query(option, list_conf, list_conf_data)

        #FIXME 存在可能的性能问题
        for f in option['field']:
            field_func, field_obj, field_item = self._parse_item(f)
            # 处理开发者字段函数 
            object_name = option['object'] 
            if field_obj != self.default_object:
                object_name = field_obj
            field_map = self.name_schema[object_name].get(field_item) or {}
            if field_map.get('output_func'):
                output_func = getattr(self, field_map['output_func'], None)
                if output_func:
                    for i in result_data:
                        key = field_item
                        if field_obj != self.default_object:
                            key = f'{field_obj}__{field_func}'
                        i[key] = output_func(i[key])

            # 处理调用方字段函数
            if field_func != self.default_func:
                for i in result_data:
                    key = field_item
                    if field_obj != self.default_object:
                        key = f'{field_obj}__{field_func}'
                    i[key] = self._query_field_func(field_func, i[key])


        if self.pagination:
            self.r['list'] = result_data
        elif self.select_one:
            self.r = result_data[0] if result_data else {}
        else:
            self.r = result_data

        for i in self.after:
            after_func = getattr(self, i, None)
            if not after_func:
                continue
            after_func(self.r)

        return self.r

    def _input_func(self, object_data, object_name):
        # inout func
        schema_map = self.name_schema[object_name]
        for k, v in object_data.items():
            if k not in schema_map:
                continue
            input_func = schema_map[k].get('input_func')
            if not input_func:
                continue
            input_func = getattr(self, input_func, None)
            if not input_func:
                continue
            object_data[k] = input_func(v)
        return object_data 

    def update(self, *args, **kw):

        # 参数验证
        option = self.run_builder(UpdateData)
        option['data'] = self._input_func(option['data'], option['object'])

        with get_connection(option['namespace']) as db:
            db.update(
                table = option['object'],
                values = option['data'],
                where = option['setting']['by']
            )

    def _default(self, value):
        return value

    def create(self, *args, **kw):

        # 参数验证
        option = self.run_builder(CreateData)
        option_datas = []
        for i in option['data']:
            option_datas.append(self._input_func(i, option['object']))
        option['data'] = option_datas

        self._option_setting(option)
        log.debug(f'call before: {self.before}')

        for d in option['data']:
            for i in self.before:
                func = getattr(self, i, self._default)
                func(d)

        insert_id = None
        with get_connection(option['namespace']) as db:
            db.insert_list(
                table = option['object'],
                values_list = option['data'],
            )
            insert_id = db.last_insert_id()

        for a in self.after:
            func = getattr(self, i, self._default)
            func(option['data'], insert_id)

        return insert_id

    def _meta_object(self, table):
        schema = self.name_schema.get(table)
        schema = copy.deepcopy(schema)
        if not schema:
            raise ParamError('查询的表定义不存在')
        table_code = schema.pop('_name')
        table_name = schema.pop('_name_descr', table_code)

        for k, v in schema.items():
            v['code'] = k

        fields = list(schema.values())
        return {'name': table_name, 'fields': fields}

    def meta(self, *args, **kw):

        # 参数验证
        option = self.run_builder(MetaData)
        self._option_setting(option)
        table = option.get('object')
        if table:
            return self._meta_object(table)
        else:
            return [i['_name'] for i in self.schemas]
