### Base image tasks
FROM fabric8/java-centos-openjdk8-jdk

USER root

RUN yum -y update && yum install -y software-properties-common \
    git-core \
    openssh-client \
    vim \
    unzip \
    wget

RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm
RUN yum -y install https://centos7.iuscommunity.org/ius-release.rpm
RUN yum -y update
RUN yum -y install python36u python36u-libs python36u-devel python36u-pip google-chrome-stable_current_x86_64.rpm

# Found on: https://liguoliang.com/deault/2018/08/18/selenium-with-headless-chrome-with-centos-docker.html
RUN mkdir /opt/chrome
RUN curl -O https://chromedriver.storage.googleapis.com/2.41/chromedriver_linux64.zip
RUN unzip chromedriver_linux64.zip -d /opt/chrome

ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME/bin:$PATH
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
