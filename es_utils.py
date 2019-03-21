#!/usr/bin/python
# -*- coding: UTF-8 -*-
# another: Jeff.Chen
# elasticsearch读写工具类
import time
import pytz
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch


# 固定索引前缀
INDEX_PREFIX = 'hbase-calltest-'
index_name = INDEX_PREFIX + time.strftime("%Y.%m.%d", time.gmtime(time.time()))


class EsUtils(object):
    def __init__(
            self,
            es_list=['0.0.0.0:9200'],
            es_user=None,
            es_pw=None,
            *args, **kwargs):
        self._es_list = es_list
        if es_user and es_pw:
            self._es = Elasticsearch(es_list, http_auth=(es_user, es_pw))
        else:
            self._es = Elasticsearch(es_list)

    def write(self, doc, index=index_name):
        '''写入es'''
        res = self._es.index(
            index=index_name,
            doc_type='metric',
            body=doc
        )


if __name__ == '__main__':
    write_timestamp = '1553173047000'
    # 当前写入时间
    ct = time.time()
    # 以东八区写入，格式："2017-01-22T12:58:17.136+0800"
    t = datetime.fromtimestamp(round(ct, 3), pytz.timezone(
        'Asia/Shanghai')).strftime('%Y-%m-%dT%H:%M:%S.%f%z')

    doc = {
        'write_time': datetime.fromtimestamp(int(write_timestamp[:-3])),
        'no': '120',
        'response_time': 20,
        'timestamp': t
    }

    es_utils = EsUtils(es_list=['0.0.0.0:9200'])
    es_utils.write(doc=doc)
