#!/usr/bin/python
# -*- coding: UTF-8 -*-
# another: Jeff.Chen
# HBase拨测工具类
import hbase
import sys
import time
import pytz
from es_utils import EsUtils
from datetime import datetime, timedelta


# HBase建表模型
HBASE_NS = 'calltest'
HBASE_CF = 'heartbeat'


class HbaseCalltest(object):
    '''HBase拨测工具类'''

    def __init__(self, zk_conf='0.0.0.0:2181', *args, **kwargs):
        self._zk_conf = zk_conf
        self._conn = None
        self._es_utils = None

    @property
    def zk_conf(self):
        return self._zk_conf

    @zk_conf.setter
    def zk_conf(self, zk_conf):
        self._zk_conf = zk_conf

    @property
    def es_utils(self):
        return self._es_utils

    @es_utils.setter
    def es_utils(self, es_utils):
        if isinstance(es_utils, EsUtils):
            self._es_utils = es_utils
        else:
            raise TypeError('es_utils类型异常')

    def get_conn(self):
        '''获取hbase连接'''
        if self._conn is None:
            self._conn = hbase.ConnectionPool(self._zk_conf).connect()

        return self._conn

    def conn_close(self):
        '''关闭hbase连接'''
        if self._conn:
            self._conn.close()

        pass

    def run_task(self):
        '''运行拨测任务：读后，NO+1写'''
        last_cf_no = 0
        last_write_time = 0
        try:
            table = self.get_conn()[HBASE_NS][HBASE_CF]

            # 只扫描最近10条
            for row in table.scan(batch_size=10):
                # print(row)
                last_cf_no = int(str(row['cf:no'], encoding='utf-8'))
                last_write_time = sys.maxsize - int(row.key)
                break
            curr_cf_no = last_cf_no + 1
            self.write_new_row(cf_no=curr_cf_no, table=table)
            self.send_metric(curr_cf_no=curr_cf_no, last_write_time=last_write_time)
        except Exception as e:
            raise RuntimeError("run_task异常：" + str(e))

    def write_new_row(self, cf_no, table):
        '''生成新记录，在原no上+1'''
        curr_rowkey = generate_rowkey()
        table.put(hbase.Row(
            str(curr_rowkey), {
                'cf:no': str(cf_no).encode()
            }
        ))

    def send_metric(self, curr_cf_no, last_write_time):
        '''发送指标'''
        if self._es_utils:
            self._es_utils.write(get_es_doc(
                cf_no=curr_cf_no,
                last_write_time=last_write_time
            ))
        pass


def get_es_doc(cf_no, last_write_time):
    '''生成写入es的doc
    :param cf_no: 当前自增序号
    :param last_write_time: 上一次写入HBASE的时间戳
    '''
    # 当前写入时间
    ct = time.time()
    # 以东八区写入
    t = datetime.fromtimestamp(round(ct, 3), pytz.timezone(
        'Asia/Shanghai')).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
    # 响应时间 = 当前时间 - 上次写入时间
    rt = int(ct) - int(last_write_time)
    doc = {
        'last_write_time': datetime.fromtimestamp(int(last_write_time)),
        'no': int(cf_no),
        'response_time': rt,
        'timestamp': t
    }
    return doc


def generate_rowkey():
    '''生成唯一rowkey：最大Long-当前时间戳'''
    now_timestamp = int(time.time())
    return sys.maxsize - now_timestamp


if __name__ == '__main__':
    demo = HbaseCalltest(zk_conf='0.0.0.0:2181')
    demo.es_utils = EsUtils(es_list=['0.0.0.0:9200'])
    demo.run_task()
    demo.conn_close()
