#!/bin/sh

echo "Downloading go."
wget --quiet https://golang.org/dl/go1.15.5.linux-amd64.tar.gz -O tmp.tar.gz

echo "Installing go."
tar -C `pwd` -xzf tmp.tar.gz
export PATH=`pwd`/go/bin:$PATH

echo "Getting the nats-server"
go get github.com/nats-io/nats-server/v2@398ef78aac066a54867d10b73fccf50089de0d02

# nats-server should be in `pwd`/go/bin
cp `pwd`/bin/nats-server .

echo "NATS server version"
`pwd`/nats-server --version

