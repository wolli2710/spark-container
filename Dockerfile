### Base image tasks
FROM fabric8/java-centos-openjdk8-jdk

USER root

RUN yum -y update && yum install -y software-properties-common \
    git-core \
    openssh-client \
    openssh-server \
    vim \
    wget
RUN yum update -y libselinux

# passwordless ssh
RUN ssh-keygen -q -N "" -t dsa -f /etc/ssh/ssh_host_dsa_key
RUN ssh-keygen -q -N "" -t rsa -f /etc/ssh/ssh_host_rsa_key
RUN ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa
RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

RUN yum -y install https://centos7.iuscommunity.org/ius-release.rpm
RUN yum -y update
RUN yum -y install python36u python36u-libs python36u-devel python36u-pip

ENV HADOOP_PREFIX /usr/local/hadoop
ENV HADOOP_COMMON_HOME /usr/local/hadoop
ENV HADOOP_HDFS_HOME /usr/local/hadoop
ENV HADOOP_MAPRED_HOME /usr/local/hadoop
ENV HADOOP_YARN_HOME /usr/local/hadoop
ENV HADOOP_CONF_DIR /usr/local/hadoop/etc/hadoop

ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME/bin:$PATH
ENV PYTHONPATH $SPARK_HOME$\python;$SPARK_HOME$\python\lib\py4j-0.10.7-src.zip

# ENV PYSPARK_SUBMIT_ARGS "--jars path/to/hive/jars/jar.jar,path/to/other/jars/jar.jar --conf spark.driver.userClassPathFirst=true --master local[*] pyspark-shell"

ENV PYSPARK_PYTHON /usr/bin/python3.6
RUN rm -f /usr/bin/python && ln -s /usr/bin/python3.6 /usr/bin/python

RUN cd /opt && \
    wget http://www-eu.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz && \
    tar -xzf spark-2.4.3-bin-hadoop2.7.tgz && \
    ln -s /opt/spark-2.4.3-bin-hadoop2.7  /opt/spark

WORKDIR /data/

ADD ./ /data/

RUN pip3.6 install --upgrade pip && pip install -r /data/requirements.txt

################################################################################################################
#########################################            HADOOP            ##########################################
#################################################################################################################

RUN wget --directory-prefix=/tmp/ http://mirror.klaus-uwe.me/apache/hadoop/common/hadoop-2.7.6/hadoop-2.7.6.tar.gz
RUN tar xvzf /tmp/hadoop-2.7.6.tar.gz -C /tmp/

RUN ln -s /tmp/hadoop-2.7.6 /usr/local/hadoop

RUN chmod -R 777 /usr/local/hadoop/etc/hadoop

RUN $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh && $HADOOP_PREFIX/sbin/start-dfs.sh && $HADOOP_PREFIX/bin/hdfs dfs -mkdir -p /user/root

# RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
# RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
# RUN chmod 0600 ~/.ssh/authorized_keys

RUN $HADOOP_PREFIX/bin/hdfs namenode -format
RUN $HADOOP_PREFIX/sbin/start-dfs.sh

CMD python3.6 -W ignore -m unittest /data/tests/test_*.py


# Hdfs ports
EXPOSE 50010 50020 50070 50075 50090 8020 9000
# Mapred ports
EXPOSE 10020 19888
#Yarn ports
EXPOSE 8030 8031 8032 8033 8040 8042 8088
#Other ports
EXPOSE 49707 2122
