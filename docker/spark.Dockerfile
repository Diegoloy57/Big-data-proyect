FROM apache/spark:3.5.3

USER root

RUN pip install --no-cache-dir \
    pymongo \
    pandas

RUN mkdir -p /opt/spark/.ivy2 \
    && chown -R spark:spark /opt/spark/.ivy2

WORKDIR /app

COPY . /app

USER spark

ENV SPARK_SUBMIT_OPTS="-Divy.cache.dir=/opt/spark/.ivy2/cache -Divy.home=/opt/spark/.ivy2"