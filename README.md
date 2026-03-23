这是一个实时日志采集与智能告警平台。
基于Spring Boot3，Netty，Kafka，Redis，DuckDB，Spring Cloud Gateway，Nacos，Vue3的微服务运维监控系统。
环境配置：
Java 17+
Maven 3.9+
Docker Desktop

如何启动：
1.docker compose up -d
2.创建Kafka Topic
docker exec -it log-kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic raw-logs \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
3.mvn install -DskipTests
4.进入四个层级+一个测试demo进行mvn spring-boot:run即可

注意：如果想使用邮件功能，请先获取授权码，至于步骤，搜一下就有。
