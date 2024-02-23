import logging
import re
import sys
import json
import traceback

from mtools.utils.valid import is_valid_int as is_int

log = logging.getLogger()


def get_fn(module, fn_name):
    if isinstance(module, str):
        module = sys[module]

    return getattr(module, fn_name, None)


def to_list(x):
    if not isinstance(x, (list, tuple, set)):
        return [x]
    return x


def to_id_list(s):
    if isinstance(s, (list, tuple, set)):
        return list({int(i) for i in s})
    return list({
        int(i.strip())
        for i in re.split('\\D', s or '') if is_int(i.strip())
    })


def gen_match(*args):
    if not args:
        return ''
    return '(%s)' % ('|'.join(map(str, args)))


def safe(func):
    def _(*args, **kw):
        try:
            return func(*args, **kw)
        except:
            return

    return _


safe_json_loads = safe(json.loads)
safe_int = safe(int)


def fill_dict(d, keys=None, value=None, **kw):
    """填充dict数据"""
    ret = {key: value for key in (keys or [])}
    ret.update(kw)
    ret.update(d or {})
    return ret


def ydict(*d, val_not_in=None, pick_keys=None):
    not_in = to_list(val_not_in)
    keys = None if pick_keys is None else set(to_list(pick_keys))
    ret = {}
    for item in d:
        ret.update(item or {})
    return {
        k: v for k, v in ret.items()
        if v not in not_in and (not keys or k in keys)
    }



def get_value(d, *keys, _default=None):
    data = d or {}
    for k in keys:
        if data and k in data:
            data = data[k]
        else:
            return _default
    return data


if __name__ == '__main__':
    print(get_value({'123': 1}, '123', 1))
    print(get_value(*[{'1': {'2': '123'}}, '1', '2']))
    print(get_value(None, 1, 2, 3, _default=123))
    print(ydict({'name': 1}, {'age': 20}, {'email': '123@qq.com'}, pick_keys=['name', 'age']))
    print(safe_json_loads('a123'))
    # print(json.loads('a123'))
    safe_int('12')
    print(merge_dict(None, None))
