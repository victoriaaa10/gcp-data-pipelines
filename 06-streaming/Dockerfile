FROM flink:1.19-java17

# Install Python + JDK
RUN apt-get update -y && \
    apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3 /usr/bin/python

# Symlink JDK include headers into the path pemja expects (arm64)
RUN ln -s /usr/lib/jvm/java-17-openjdk-arm64/include /opt/java/openjdk/include

# Copy connector JARs into Flink's lib directory
COPY lib/*.jar /opt/flink/lib/

# Upgrade pip
RUN pip install --upgrade pip

# Install PyFlink and dependencies
RUN pip install --no-cache-dir \
    apache-flink==1.19.0 \
    kafka-python \
    pandas \
    pyarrow \
    ruamel.yaml

WORKDIR /opt/src

CMD ["tail", "-f", "/dev/null"]