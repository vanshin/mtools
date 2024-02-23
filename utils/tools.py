import re
import sys
import time
import urllib
import calendar
import logging
import demjson3
import threading
import traceback
from datetime import datetime, timedelta, date

from mtools.resp.define import DTM_FMT
from mtools.resp.excepts import ParamError

UN_ID = 0

log = logging.getLogger()


def str_timedelte(v):

    v = ':'.join(
        i.zfill(2)
        for i in str(v).split(':')[:2]
    )
    return v


def to_list(x):
    if not isinstance(x, (list, tuple, set)):
        return [x]
    return x

def dt_to_str(self):
    pass


def get_fn(module, fn_name):
    if isinstance(module, str):
        module = sys[module]

    return getattr(module, fn_name, None)


def get_value(d, *keys):
    data = d or {}
    for k in keys:
        if data:
            data = data.get(k)
        else:
            return None
    return data

decimal_re = re.compile(r"decimal\('(\d+(?:\.\d+)?)'\)")
decimal_re_p = re.compile(r"Decimal\('(\d+(?:\.\d+)?)'\)")
datetime_re = re.compile(r'datetime\.datetime\((\d{4}),\s*(\d{1,2}),\s*(\d{1,2}),\s*(\d{1,2}),\s*(\d{1,2}),\s*(\d{1,2})(?:,\s*(\d{1,6}))?\)')


def json_clear_bytes(params):

    if params[0] == 'b':
        params = params[1:]

    while params[0] == params[-1]:
        params = params[1:-1]
    return params


def parse_params(params, to_dict=False):
    if not params:
        return {}

    if params[0] in ('{', '[') and params[-1] in (']', '}'):

        try:
            params = demjson3.decode(
                params.encode('raw_unicode_escape'),
                encoding='utf8'
            )
        except demjson3.JSONDecodeError as e:
            try:
                if 'True' in params:
                    params = params.replace('True', 'true')
                if 'False' in params:
                    params = params.replace('False', 'false')
                if 'None' in params:
                    params = params.replace('None', 'null')
                if 'decimal' in params:
                    params = decimal_re.sub(r'\1', params)
                if 'Decimal' in params:
                    params = decimal_re_p.sub(r'\1', params)
                if 'datetime' in params:
                    params = datetime_re.sub(r'"\1-\2-\3 \4:\5:\6.\7"', params)
                params = demjson3.decode(
                    params.encode('raw_unicode_escape'),
                    encoding='utf8'
                )
            except Exception:
                log.warn(params)
                log.warn(traceback.format_exc())
                return {}
        except Exception:
            log.warn(params)
            log.warn(traceback.format_exc())
            return {}
    elif '=' in params and '{' not in params and '}' not in params:
        try:
            params = urllib.parse.parse_qsl(params)
            params = dict(params)
        except Exception:
            log.warn(traceback.format_exc())
            return {}

    if isinstance(params, str) and to_dict:
        return {'data': params}

    return params


def now_seconds():

    now = datetime.now()
    today = datetime(now.year, now.month, now.day)
    return (now - today).seconds


def pad_with_zeros(original_str, width):
    return original_str.zfill(width)


thread_lock = threading.Lock()


def synchronize(func):
    def _(*args, **argitems):
        thread_lock.acquire()
        x = None
        try:
            x = func(*args, **argitems)
        finally:
            thread_lock.release()
        return x
    return _


@synchronize
def generate_un():
    """生成六位id
    """
    global UN_ID
    UN_ID += 1
    if UN_ID >= 100000:
        UN_ID = 0
    padded_str = pad_with_zeros(str(UN_ID), 5)
    return padded_str


def create_syssn(work_id=0):
    # 获取当前日期信息
    now = datetime.now().strftime("%Y%m%d%H%M%S")

    # 生成17位随机数字字符串作为订单号的一部分
    random_string = generate_un()

    # 组合订单号，格式为：年月日 + 随机数字字符串
    order_number = f"{now}{str(work_id)}{random_string}"

    return order_number


def decode_datetime_str(datetime_str: str, formats):
    if not isinstance(formats, list):
        formats = [formats]
    for f in formats:
        try:
            return datetime.strptime(datetime_str, f)
        except ValueError:
            pass
    return None


def to_datetime(x):
    """将datetime.date/datetime.datetime/str/number转为datetime.datetime"""
    if isinstance(x, date):
        return datetime(year=x.year, month=x.month, day=x.day)
    elif isinstance(x, datetime):
        return x
    elif isinstance(x, str):
        if not decode_datetime_str(x, [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%d',
        ]):
            raise ParamError('数据不能转换为datetime')
    elif isinstance(x, (float, int)):
        return datetime.fromtimestamp(x)
    raise ParamError('数据不能转换为datetime')


def datetime_to_str(self, dt=None):
    dt = dt or datetime.now()
    return dt.strftime(DTM_FMT)


def future(
        st=None, years=0, months=0, weeks=0,
        days=0, hours=0, minutes=0, seconds=0,
        milliseconds=0, microseconds=0, fmt_type='date',
        fmt=DTM_FMT, **kw
):
    """ 相对时间
    Params:
        st: 起始时间, datetime或者date类型
        years, months...: 时间, 负的为向前推算
        fmt_type: str,返回fmt字符串
                  timestamp,返回时间戳
                  date,返回datetime或者date类型
    """
    st = st or datetime.now()
    if not isinstance(st, datetime):
        raise ParamError('时间格式不正确')

    if seconds or minutes or hours or days or weeks:
        st += timedelta(
            weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds,
            milliseconds=milliseconds, microseconds=microseconds
        )

    if months:
        addyears, months = divmod(months, 12)
        years += addyears
        if not (1 <= months + st.month <= 12):
            addyears, months = divmod(months + st.month, 12)
            months -= st.month
            years += addyears
    if months or years:
        year = st.year + years
        month = st.month + months
        try:
            st = st.replace(year=year, month=month)
        except ValueError:
            _, destination_days = calendar.monthrange(year, month)
            st = st.replace(year=year, month=month, day=destination_days)

    if fmt_type == 'str':
        return st.strftime(fmt)
    elif fmt_type == 'timestamp':
        return time.mktime(st.timetuple())
    else:
        return st

    return st


def seconds_to_time(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    time_str = ""
    if hours > 0:
        time_str += f"{hours}小时"
    if minutes > 0:
        time_str += f"{minutes}分钟"
    if seconds > 0:
        time_str += f"{seconds}秒"

    return time_str


@synchronize
def create_pick_code():
    return counter.get_next_id()

