# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import json
import hashlib


class Utils(object):
    @staticmethod
    def get_today_date():
        return datetime.now().strftime('%Y%m%d')

    @staticmethod
    def get_yesterday_date():
        today = datetime.now()
        delta = timedelta(days=-1)
        yesterday = today + delta
        return yesterday.strftime('%Y-%m-%d')

    @staticmethod
    def get_batch():
        today = datetime.now()
        delta = timedelta(days=-1)
        yesterday = today + delta
        return yesterday.strftime('%Y%m%d')

    @staticmethod
    def parse_corp_info(corp_list):
        try:
            corp_json = json.loads(corp_list)[0]
            corp_name = corp_json['name']
            corp_id = corp_json['corp_id']
            return {"corp_name": corp_name, "corp_id": corp_id}
        except:
            return {"corp_name": None, "corp_id": None}

    @staticmethod
    def gen_uuid(a, b):
        t = ''.join(sorted([a, b]))
        m = hashlib.md5()
        m.update(bytes(str(t)))
        return m.hexdigest()

    @staticmethod
    def json_to_string(s):
        t = json.dumps(s)
        r = t.replace('"', "'")
        return r
