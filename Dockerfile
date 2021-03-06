FROM golang:1.10 AS builder

RUN \
    apt-get update && \
    apt-get install -y build-essential

RUN git clone -b v0.11.3 https://github.com/edenhill/librdkafka

RUN \
    cd librdkafka && \
    ./configure && \
    make && \
    make install

WORKDIR $GOPATH/src/github.com/mikechris/kaha

COPY . ./

RUN GOOS=linux go build -ldflags "-X main.version=`date -u +%Y%m%d.%H%M%S`" -tags static -o /kaha .

FROM debian:stretch-slim

RUN \
    apt-get update && \
    apt-get install -y librdkafka1 && \
    rm -fr /tmp/* /var/tmp/* /var/lib/apt/lists/*

COPY --from=builder /kaha /bin/kaha

ENTRYPOINT ["/bin/kaha"]