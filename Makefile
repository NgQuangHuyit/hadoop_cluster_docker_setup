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


build-hadoopbase:
	docker build -t hadoopbase -f ./hadoop-base/Dockerfile ./hadoop-base

