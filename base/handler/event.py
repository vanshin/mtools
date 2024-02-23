import gevent
import logging
import requests
import traceback

from mtools.base.handler import BuildHandler
from mtools.base.domain import kv, EventDomain, EventSubscriptionDomain
from mtools.define.event import EVENT_PUBLIC, EVENT_OPEN, SUB_OPEN

log = logging.getLogger()


class EventInputHandler(BuildHandler):
    """外部事件进入处理Handler"""

    def _notify_event(self, app_code, json_data):

        plat_api = kv.gv(key='PLAT_API') or 'http://127.0.0.1:7800'

        send_succ = True
        try:
            requests.post(f'{plat_api}/{app_code}/notify', json=json_data)
        except Exception:
            log.warn(traceback.format_exc())
            send_succ = False
        finally:
            log.info(f'app={app_code}|msg={json_data}|send_status={send_succ}')

    def notify_event(self, app_code, event_code, content):
        """发送给订阅事件的app"""

        if not (app_code and event_code):
            log.warn(f'app_code:{app_code} event_code:{event_code}')
            return

        evtd = EventDomain()
        event = evtd.get(
            public=EVENT_PUBLIC,
            state=EVENT_OPEN,
            app_code=app_code,
            event_code=event_code,
        )

        # 判断事件是否注册过
        if not event:
            evtd.create(
                app_code=app_code, event_code=event_code,
                public=EVENT_PUBLIC, state=EVENT_OPEN,
                descr='auto register event')
            return 'success'

        # 查询订阅这个消息的用户
        subs = EventSubscriptionDomain().gets(
            event_id=event['id'],
            state=SUB_OPEN
        )

        for i in subs:
            json_data = {
                'from_app': app_code,
                'event_code': event_code,
                'content': content
            }
            sp = gevent.spawn(
                self._notify_event, i['app_code'], json_data
            )
            sp.start()
