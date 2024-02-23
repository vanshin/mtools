CONFIG_STATES = (CONF_OPEN, CONF_CLOSE) = (1, 2)

VERSION_USING = (USING, USED, UNUSED) = (1, 2, 3)


BIND_TYPES = (APP_BIND, FEATURE_BIND, CATE_BIND, APPLIED) = (1, 2, 3, 4)
FEATURE_STATUS = (FEATURE_VALID, FEATURE_INVALID) = (1, 2)
BIND_STATE = (BIND_VALID, BIND_INVALID) = (1, 2)

# 是否有效
IS_VALID_TRUE = 1
IS_VALID_FALSE = 0
IS_VALID_ENUM = (
    (IS_VALID_TRUE, '有效'),
    (IS_VALID_FALSE, '无效'),
)

# app 应用状态
STATE_ON = 1
STATE_OFF = 2
STATE_ENUM = (
    (STATE_ON, '启用'),
    (STATE_OFF, '关闭'),
)
# app 是否公开
PUBLIC_YES = 1
PUBLIC_NO = 2
PUBLIC_ENUM = (
    (PUBLIC_YES, '公开'),
    (PUBLIC_NO, '私有'),

)

# 推送方式
NOTIFY_TYPES = set(['email', 'msg', 'feishu'])
NOTIFY_TYPES_EMAIL = 'email'
NOTIFY_TYPES_MSG = 'msg'
NOTIFY_TYPES_FEISHU = 'feishu'
NOTIFY_TYPES_ENUM = (
    (NOTIFY_TYPES_EMAIL, '邮件'),
    (NOTIFY_TYPES_MSG, '短信'),
    (NOTIFY_TYPES_FEISHU, '飞书')
)


# 2开头的错误代码第二位代表错误等级
# 0. 严重错误; 1. 普通错误; 2. 规则错误; 3. 一般信息; 4. 未知错误
# 3 开头代表业务错误
# 31XX 代表用户信息相关错误
OK = "0000"
TIMESERR = "1016"
STOREDEVICEERR = "1010"
DBERR = "2000"
THIRDERR = "2001"
SESSIONERR = "2002"
DATAERR = "2003"
IOERR = "2004"
LOGINERR = "2100"
PARAMERR = "2101"
USERERR = "2102"
ROLEERR = "2103"
PWDERR = "2104"
VCODERROR = "2105"
PERMERR = "2106"
BEENBOUND = "2107"
UNBOUNDED = "2108"
CROSSERR = "2109"
NEVERREPEAT = "2110"
NOASSOCIATED = "2111"
NODEVICE = "2112"
REQERR = "2200"
IPERR = "2201"
MACERR = "2202"
NODATA = "2300"
DATAEXIST = "2301"
UNKOWNERR = "2400"
USERNOPRESCERR = "2704"
HTTPRESULTERR = "2501"
HTTPCALLERR = "2501"

# 用户信息相关错误
NOUSER = '3100'
OPENUSER_UNBIND = '3101'
COMLETE_USERINFO = '3102'
WXTOKEN_OVERDUE = '3103'

error_map = {
    OK: u"成功",
    DBERR: u"数据库查询错误",
    STOREDEVICEERR: "门店设备不匹配",
    THIRDERR: u"第三方系统错误",
    SESSIONERR: u"用户未登录",
    DATAERR: u"数据错误",
    IOERR: u"文件读写错误",
    LOGINERR: u"用户登录失败",
    PARAMERR: u"参数错误",
    USERERR: u"用户不存在或未激活",
    ROLEERR: u"用户身份错误",
    PWDERR: u"密码错误",
    VCODERROR: u"验证码错误",
    REQERR: u"非法请求或请求次数受限",
    IPERR: u"IP受限",
    MACERR: u"MAC校验失败",
    NODATA: u"无数据",
    DATAEXIST: u"数据已存在",
    UNKOWNERR: u"未知错误",
    PERMERR: u"用户无权限访问",
    BEENBOUND: u"设备已被绑定",
    UNBOUNDED: u"用户未绑定设备",
    CROSSERR: u"数据越界",
    NEVERREPEAT: u"不能重复",
    NOASSOCIATED: u"未关联",
    NODEVICE: u"设备不存在",

    NOUSER: '用户不存在',
    OPENUSER_UNBIND: '未绑定',
    COMLETE_USERINFO: '用户信息不完善',
    WXTOKEN_OVERDUE: '微信token不存在或已过期',
    TIMESERR: '用户次数不足',
    USERNOPRESCERR: "用户没有处方",
}


def get_errmsg(code):
    return error_map.get(code, "未知错误")

# 默认数据库
DEFAULT_DATABASE = "comistxs"


TIME_FMT = '%H:%M:%S'
DT_FMT = '%Y-%m-%d'
DTS_FMT = '%Y-%m-%d 00:00:00'
DTM_FMT = '%Y-%m-%d %H:%M:%S'
DTMF_FMT = '%Y-%m-%d %H:%M:%S,%f'


# PLAT_API
PLAT_API = 'http://127.0.0.1:7800'
