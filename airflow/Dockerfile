FROM apache/airflow:2.8.1-python3.10

COPY requirements.txt /requirements.txt
COPY entrypoint.sh /entrypoint.sh

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

USER root

# Install basic tools needed to add repositories
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    gnupg2 \
    ca-certificates \
    software-properties-common && \
    rm -rf /var/lib/apt/lists/*

# Add the JDK repository (if not available)
RUN echo "deb http://deb.debian.org/debian bullseye main contrib non-free" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*
    
#  Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Gives permission to the entrypoint
RUN chmod +x /entrypoint.sh

USER airflow
ENTRYPOINT ["/entrypoint.sh"]
