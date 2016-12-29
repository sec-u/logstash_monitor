# Logstash 客户端监控

## 功能

* Logstash 客户端心跳检测，利用 Elasticsearch 复合查询进行监测掉线的主机；
* 检测到掉线主机数据发送到 Elasticsearch，配置 Kibana 进行前端展示；
* 检测到的掉线主机，如果有用户名和密码则进行自动拉起 Logstash 客户端；

## 环境说明

* Python 2.7.5
* Elasticsearch 2.4.0 (Python 库)

## 使用方法

```
$ python main.py
```