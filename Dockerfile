FROM debian:jessie

ENV LIBRDKAFKA_TAG=v0.11.1
ENV GOLANG_VERSION=1.9.2

RUN \
    apt-get update && \
    apt-get install -y \
                        build-essential \
                        git-core \
                        libsasl2-dev \
                        libssl-dev \
                        pkg-config \
                        python \
                        wget \
                        zlib1g-dev

RUN \
    git clone -b $LIBRDKAFKA_TAG https://github.com/edenhill/librdkafka && \
    cd librdkafka && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm -fr librdkafka

RUN \
    wget https://redirector.gvt1.com/edgedl/go/go$GOLANG_VERSION.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go$GOLANG_VERSION.linux-amd64.tar.gz && \
    rm -f go$GOLANG_VERSION.linux-amd64.tar.gz

ENV PATH=/usr/local/go/bin:$PATH

RUN \
    go version