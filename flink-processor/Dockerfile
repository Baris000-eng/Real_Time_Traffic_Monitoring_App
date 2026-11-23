FROM flink:1.18-scala_2.12-java17

RUN apt-get update && \
    apt-get install -y \
    python3 \
    python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

COPY requirements.txt .

RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt

COPY flink-sql-connector-kafka-3.0.1-1.18.jar /opt/flink/lib/

COPY . .

ENV PYTHONUNBUFFERED=1



