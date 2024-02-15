start-ssh-master:
	docker-compose exec hadoopmaster /bin/bash -c "service ssh start"

start-ssh-slave1:
	docker-compose exec hadoopslave1 /bin/bash -c "service ssh start"

start-ssh-slave2:
	docker-compose exec hadoopslave2 /bin/bash -c "service ssh start"

start-ssh: start-ssh-master start-ssh-slave1 start-ssh-slave2



mkdir-spark-logs: 
	docker-compose exec hadoopmaster /bin/bash -c "hdfs dfs -mkdir -p /spark-logs"

start-spark-history-server:
	docker-compose exec spark-client /bin/bash -c "start-history-server.sh"

stop-spark-history-server:
	docker-compose exec spark-client /bin/bash -c "stop-history-server.sh"


format-hdfs:
	docker-compose exec hadoopmaster /bin/bash -c "hdfs namenode -format"
start-hadoop:
	docker-compose exec hadoopmaster /bin/bash -c "start-all.sh"
stop-hadoop:
	docker-compose exec hadoopmaster /bin/bash -c "stop-all.sh"
restart-hadoop: stop-hadoop start-hadoop



check-master:
	docker-compose exec hadoopmaster /bin/bash -c "jps"
check-slave1:
	docker-compose exec hadoopslave1 /bin/bash -c "jps"
check-slave2:
	docker-compose exec hadoopslave2 /bin/bash -c "jps"
check: check-master check-slave1 check-slave2


stop-compose:
	docker-compose stop

start-compose:
	docker-compose start

stop: stop-spark-history-server stop-hadoop stop-compose


start: start-compose start-ssh


build-hadoopbase:
	docker build -t hadoopbase -f ./hadoop-base/Dockerfile ./hadoop-base

compose-up:
	docker-compose up -d

compose-down:
	docker-compose down --volume

init: start-ssh format-hdfs

up:  build-hadoopbase compose-up init

down: compose-down
