# hbase-calltest-webapp
HBase拨测监控Web服务

## 功能

* 读取集群的ZK地址，获取HBase访问连接
* 尝试读写，计算耗时，生成指标
* 指标写入给定的ES地址
* 支持swagger，详细功能见接口定义
  
### 建议

* 通过推送拨测监控目标的参数，启动服务
* 后续的告警事件触发，可以由ELK配套的ElastAlert，或者Grafana的Alert来实施

## 运行

* 基于py3
* 导入依赖`pip install -r requirement.txt`
* 启动，默认端口5000`nohup python app.py -p <port> -e <dev(默认prod)> >/dev/null 2>&1 &`
* 访问`http://0.0.0.0:5000`
  * `/conf`：配置任务参数
  * `/run`：任务启动或停止