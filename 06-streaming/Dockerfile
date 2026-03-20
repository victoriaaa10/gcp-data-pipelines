FROM flink:1.19-java17

RUN apt-get update -y && \
    apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3 /usr/bin/python

# Symlink arm64 JDK include path into pemja's expected location
RUN ln -s /usr/lib/jvm/java-17-openjdk-arm64/include /opt/java/openjdk/include

# Upgrade pip
RUN pip install --upgrade pip

# Install PyFlink and dependencies
RUN pip install --no-cache-dir \
    apache-flink==1.18.0 \
    kafka-python \
    pandas \
    pyarrow

WORKDIR /opt/src

CMD ["tail", "-f", "/dev/null"]