#!/bin/sh

echo "Downloading go."
wget --quiet https://golang.org/dl/go1.15.5.linux-amd64.tar.gz -O tmp.tar.gz

echo "Installing go."
tar -C `pwd` -xzf tmp.tar.gz
export GOROOT=`pwd`/go
export PATH=$GOROOT/bin:$PATH

echo "Getting the nats-server"

curdir=`pwd`
mkdir -p $GOROOT/src/github.com/nats-io/nats-server
git clone https://github.com/nats-io/nats-server.git $GOROOT/src/github.com/nats-io/nats-server
cd $GOROOT/src/github.com/nats-io/nats-server
GO111MODULE=on go install -v ./...

cd $curdir

# nats-server should be in `pwd`/go/bin
mkdir nats-server
cp $GOROOT/bin/nats-server nats-server

echo "NATS server version:"
`pwd`/nats-server/nats-server --version
