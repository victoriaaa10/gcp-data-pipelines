FROM flink:1.19-python3

RUN pip install --no-cache-dir \
    kafka-python \
    pandas \
    pyarrow

WORKDIR /opt/src

CMD ["tail", "-f", "/dev/null"]