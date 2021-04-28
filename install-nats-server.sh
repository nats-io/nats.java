git clone https://github.com/nats-io/nats-server.git
cd nats-server
go build main.go
cd ..
nats-server/main -v