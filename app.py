#!/usr/bin/python
# -*- coding: UTF-8 -*-
# another: Jeff.Chen
# Hbase的拨测Web接口，提供swagger
import sys, getopt
import time
from flask import Flask
from flask_restplus import Api, Resource, fields
import atexit
from apscheduler.schedulers.background import BackgroundScheduler
from hbase_calltest import HbaseCalltest
from es_utils import EsUtils
import logging
from logging.handlers import TimedRotatingFileHandler


# log配置
log_fmt = '%(asctime)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(log_fmt)
#创建TimedRotatingFileHandler对象
log_file_handler = TimedRotatingFileHandler(filename="app.log", when="D", interval=5, backupCount=2)
log_file_handler.setFormatter(formatter)    
logging.basicConfig(level=logging.INFO)
log = logging.getLogger()
log.addHandler(log_file_handler)


app = Flask(__name__)
api = Api(
    app, version='1.0',
    title='HBase拨测服务API',
    description='定时任务读写hbase，生成metric写入es')
ns = api.namespace('calltest', description='拨测服务')


# 拨测配置模型
conf_model = api.model('conf', {
    'zk.ip_port': fields.String('0.0.0.0:2181,0.0.0.0:2182', required=True, description='HBASE的ZK地址'),
    'es.ip_port': fields.String('0.0.0.0:9200,0.0.0.0:9201', required=True, description='ES存储地址'),
    'es.user': fields.String(required=False, description='ES账号，可选'),
    'es.password': fields.String(required=False, description='ES密码，可选'),
    'interval_seconds': fields.Integer('3', required=True, description='任务间隔时间秒'),
})


conf_list = list()
demo_conf = {
    'zk.ip_port': '0.0.0.0:2181',
    'es.ip_port': '0.0.0.0:9200',
    'es.user': 'es_username',
    'es.password': 'es_password',
    'interval_seconds': 60
}
conf_list.append(demo_conf)


# 定时任务定义
scheduler = BackgroundScheduler()
# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())


@ns.route('/conf')
class Conf(Resource):
    def get(self):
        '''获取HBase拨测服务配置'''
        return conf_list

    @ns.expect(conf_model)
    def post(self):
        '''写HBase拨测服务配置'''
        erros_msgs = list()
        if api.payload['zk.ip_port'] is None:
            erros_msgs.append('zk.ip_port不能为空')
        if api.payload['es.ip_port'] is None:
            erros_msgs.append('es.ip_port不能为空')
        if api.payload['interval_seconds'] is None:
            erros_msgs.append('interval_seconds不能为空')
        elif not str(api.payload['interval_seconds']).isdigit:
            erros_msgs.append('interval_seconds应该是正整数')

        if len(erros_msgs) > 0:
            return {'success': False, 'erros_msgs': erros_msgs}, 500
        else:
            conf_list.clear()
            conf_list.append(api.payload)
        return {'success': True}, 200


@ns.route('/run')
class Run(Resource):
    def post(self):
        '''开始运行'''
        # 读配置
        if len(conf_list) != 1:
            return 'error: 配置缺失或异常', 500

        conf = conf_list[0]
        zk_conf = conf['zk.ip_port']
        es_conf = conf['es.ip_port']
        es_user = conf['es.user']
        es_pw = conf['es.password']
        seconds = conf['interval_seconds']

        hc = HbaseCalltest(zk_conf=zk_conf)
        hc.es_utils = EsUtils(
            es_list=str(es_conf).split(','),
            es_user=es_user,
            es_pw=es_pw
        )

        if scheduler.get_jobs():
            # 清空原有任务，防止执行异常
            scheduler.remove_all_jobs()

        scheduler.add_job(func=hc.run_task,
                          trigger="interval", seconds=seconds)
        if scheduler.state != 1:
            scheduler.start()
        return 'started', 200

    def delete(self):
        '''停止运行'''
        if scheduler.state == 1:
            scheduler.remove_all_jobs()
        return 'shutdown done', 200


if __name__ == '__main__':
    env = 'prod'
    port = 5000
    try:
        args = sys.argv[1:] 
        opts, args = getopt.getopt(args,"hp:e:",["port=","env="])
    except getopt.GetoptError:
        print('app.py -p <port> -e <dev(默认prod)>]')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('app.py -p <port> -e <dev(默认prod)>]')
            sys.exit()
        elif opt in ("-p", "--port"):
            port = int(arg)
        elif opt in ("-e", "--env"):
            env = str(arg).strip()

    app.run(
        host='0.0.0.0',
        port=port,
        debug=False if str(env).lower()!='dev' else True
    )
