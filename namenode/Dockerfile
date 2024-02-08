FROM hadoopbase

RUN mkdir /hadoop-data/dfs/name \
    && echo "hadoopmaster" >> /etc/hadoop/masters \
    && service ssh start

COPY /workers /etc/hadoop/workers

CMD ["tail", "-f", "/dev/null"]