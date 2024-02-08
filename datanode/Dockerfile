FROM hadoopbase

RUN mkdir /hadoop-data/dfs/data \
    && echo "hadoopmaster" >> /etc/hadoop/masters

CMD ["tail", "-f", "/dev/null"]