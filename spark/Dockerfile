FROM hadoopbase


ARG SPARK_DOWNLOAD_URL=https://dlcdn.apache.org/spark/spark-3.4.2/spark-3.4.2-bin-hadoop3-scala2.13.tgz
RUN curl -fL ${SPARK_DOWNLOAD_URL} -o /tmp/spark \
    && tar -xvf /tmp/spark -C /opt \
    && rm -rf /tmp/spark

ENV SPARK_HOME=/opt/spark-3.4.2-bin-hadoop3-scala2.13
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PATH=$PATH:$SPARK_HOME/sbin
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH
RUN echo "export SPARK_DIST_CLASSPATH=$(hadoop classpath)" >> ${SPARK_HOME}/conf/spark-env.sh
RUN mkdir /opt/spark-data /opt/spark-apps

COPY  spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
WORKDIR /opt/spark-apps

CMD ["tail", "-f", "/dev/null"]