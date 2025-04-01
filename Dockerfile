FROM spark:python3-java17 as spark_base
USER root
ENV SPARK_HOME=/opt/spark
ADD ./requirements.txt /opt/src/requirements.txt

RUN python3 -m pip install -r /opt/src/requirements.txt

ADD ./src /src
# ADD ./src/test /test
WORKDIR /src

RUN chown -R spark:spark /src
# RUN chown -R spark:spark /test

USER spark

ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/buildd:$PYTHONPATH
