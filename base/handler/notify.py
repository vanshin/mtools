import json
import logging
import traceback
from mtools.web.validator import Field, T_STR

from mtools.resp import success
from mtools.utils.check import check
from mtools.resp.excepts import ParamError
from mtools.base.handler import BuildHandler

log = logging.getLogger()


class EventNotifyHandler(BuildHandler):

    _check_fields = [
        Field(name='from_app', valtype=T_STR, must=True),
        Field(name='event_code', valtype=T_STR, must=True),
        Field(name='content', valtype=T_STR, must=True)
    ]

    @check('must')
    def POST(self):
        process_func = getattr(
            self, f'{self.data["from_app"]}__{self.data["event_code"]}', None
        )

        if not process_func:
            raise ParamError(f'canot process event {self.data["event_code"]}')

        try:
            r = process_func(self.data['content'])
        except Exception:
            log.warn(traceback.format_exc())

        return success(r)
