### Base image tasks
FROM fabric8/java-centos-openjdk8-jdk

USER root

RUN yum -y update && yum install -y software-properties-common \
    git-core \
    openssh-client \
    vim \
    unzip \
    wget

#Install google chrome driver for selenium headless browser
RUN mkdir /tmp/chromedriver && cd /tmp/chromedriver && \
    wget https://chromedriver.storage.googleapis.com/2.35/chromedriver_linux64.zip && \
    unzip /tmp/chromedriver/chromedriver_linux64.zip chromedriver -d /usr/local/bin/

RUN yum -y install https://centos7.iuscommunity.org/ius-release.rpm
RUN yum -y update
RUN yum -y install python36u python36u-libs python36u-devel python36u-pip

ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME/bin:/usr/local/bin:$PATH
ENV PYTHONPATH $SPARK_HOME$\python;$SPARK_HOME$\python\lib\py4j-0.10.7-src.zip

ENV PYSPARK_PYTHON /usr/bin/python3.6
RUN rm -f /usr/bin/python && ln -s /usr/bin/python3.6 /usr/bin/python

RUN cd /opt && \
    wget http://www-eu.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz && \
    tar -xzf spark-2.4.3-bin-hadoop2.7.tgz && \
    ln -s /opt/spark-2.4.3-bin-hadoop2.7  /opt/spark

WORKDIR /requirements/
ADD ./requirements.txt /requirements/

RUN pip3.6 install --upgrade pip && pip install -r /requirements/requirements.txt

CMD cd /data && python3.6 -W ignore -m unittest /data/*/tests/test_*.py
