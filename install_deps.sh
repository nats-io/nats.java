#!/bin/sh

echo "Downloading go."
wget https://golang.org/dl/go1.15.3.linux-amd64.tar.gz -O tmp.tar.gz

echo "Installing go."
tar -C `pwd` -xzf tmp.tar.gz
export PATH=`pwd`/go/bin:$PATH
export GOPATH=`pwd`/go
export GOMODULE111=auto

echo "Getting the nats-server"
mkdir -p $GOPATH/src/github.com/nats-io
GO111MODULE=on go get github.com/nats-io/nats-server/v2@c0f031cc39993eb11404473411576b1cf0528b97

# nats-server should be in `pwd`/go/bin
cp `pwd`/go/bin/nats-server .

echo "NATS server version"
`pwd`/nats-server --version

