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


cd $curdir

#sudo apt-get update -y
#sudo apt-get install openjdk-8-jdk -y
#sudo update-java-alternatives --set java-1.8.0-openjdk-amd64

#wget -q https://services.gradle.org/distributions/gradle-5.5.1-bin.zip -P /tmp
#sudo unzip -d /opt/gradle /tmp/gradle-*.zip
#export GRADLE_HOME=/opt/gradle/gradle-5.5.1

#wget -q https://services.gradle.org/distributions/gradle-5.1.1-bin.zip -P /tmp
#sudo unzip -d /opt/gradle /tmp/gradle-*.zip
#export GRADLE_HOME=/opt/gradle/gradle-5.1.1

#wget -q https://services.gradle.org/distributions/gradle-6.7-bin.zip -P /tmp
#sudo unzip -d /opt/gradle /tmp/gradle-*.zip
#export GRADLE_HOME=/opt/gradle/gradle-6.7
#
#export PATH=${GRADLE_HOME}/bin:${PATH}
