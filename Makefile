start-ssh-master:
	docker-compose exec hadoopmaster /bin/bash -c "service ssh start"

start-ssh-slave1:
	docker-compose exec hadoopslave1 /bin/bash -c "service ssh start"

start-ssh: start-ssh-master start-ssh-slave1

format-hdfs:
	docker-compose exec hadoopmaster /bin/bash -c "hdfs namenode -format"

start-hdfs:
	docker-compose exec hadoopmaster /bin/bash -c "start-dfs.sh"

stop-hdfs:
	docker-compose exec hadoopmaster /bin/bash -c "stop-dfs.sh"

stop-compose:
	docker-compose stop

start-compose:
	docker-compose start

stop: stop-hdfs stop-compose

init: start-ssh format-hdfs start-hdfs

start: start-compose start-ssh start-hdfs 

build-hadoopbase:
	docker build -t hadoopbase -f ./hadoop-base/Dockerfile ./hadoop-base

compose-up:
	docker-compose up -d

compose-down:
	docker-compose down --volume

up:  build-hadoopbase compose-up init

down: stop-hdfs compose-down