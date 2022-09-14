#!/usr/bin/env bash



mkdir -p $PROJ_ROOT/conf
mkdir -p $PROJ_ROOT/tls
mkdir -p $PROJ_ROOT/state
mkdir -p $PROJ_ROOT/vault
mkdir -p $PROJ_ROOT/server_bin


# EDIT THESE
PKI="true"
TLS="true"
DOCKER="false"
SERVERNAME="nats-lab"
NATSHOST="localhost"
NATSPORT="4222"
NATSMONITORPORT="8222"
OPERATORNAME="NatsOp"
SYSTEMACCTNAME="SYS"
SYSTEMUSERNAME="System"
PREFIX=""
############


while getopts p:t:f:o: flag
do
    case "${flag}" in
        p) PKI=${OPTARG};;
        t) TLS=${OPTARG};;
        f) PREFIX="${OPTARG}_";;
        o) NATSPORT="${OPTARG}";;
    esac
done

echo "$PROJ_ROOT PKI $PKI TLS $TLS PREFIX $PREFIX"



OPERATORSERVICEURL="nats://${NATSHOST}:${NATSPORT}"
OPERATORJWTSERVERURL="nats://${NATSHOST}:${NATSPORT}"
SERVER_CONF_FILENAME="$PROJ_ROOT/conf/${PREFIX}server.conf"

echo "PKI $PKI TLS $TLS PREFIX $PREFIX SERVER_CONF_FILENAME $SERVER_CONF_FILENAME"



if [ "$PKI" = "true" ]; then
  nsc env --store "$PROJ_ROOT/vault/nats"
  nsc add operator ${OPERATORNAME}
  nsc edit operator --service-url ${OPERATORSERVICEURL} --account-jwt-server-url ${OPERATORJWTSERVERURL}
  nsc add account --name ${SYSTEMACCTNAME}
  nsc add user --account ${SYSTEMACCTNAME} --name ${SYSTEMUSERNAME}

  SYSTEMACCTPUBNKEY=`cat ${NSC_HOME}/nats/${OPERATORNAME}/accounts/${SYSTEMACCTNAME}/${SYSTEMACCTNAME}.jwt | $PROJ_BIN/util/decodejwt.sh | jq -r '.["sub"] | select( . != null )'`

  nsc edit operator --system-account ${SYSTEMACCTPUBNKEY}

  nsc add account "AcctA"
  nsc edit account --name "AcctA" --js-disk-storage=-1 --js-mem-storage=-1 --js-streams=-1 --js-consumer=-1
  nsc add user --name "UserA1" --account "AcctA"
  nsc add user --name "UserA2" --account "AcctA"

  nsc add account "AcctB"
  nsc edit account --name "AcctB" --js-disk-storage=-1 --js-mem-storage=-1 --js-streams=-1 --js-consumer=-1
  nsc add user --name "UserB1" --account "AcctB"
  nsc add user --name "UserB2" --account "AcctB"

  nsc add account "AcctC"
  nsc edit account --name "AcctC" --js-disk-storage=-1 --js-mem-storage=-1 --js-streams=-1 --js-consumer=-1
  nsc add user --name "UserC1" --account "AcctC"
  nsc add user --name "UserC2" --account "AcctC"
fi

if [ "$TLS" = "true" ]; then
  mkcert -install
  mkcert -cert-file "$PROJ_ROOT/tls/server-cert.pem" -key-file "$PROJ_ROOT/tls/server-key.pem" localhost ::1
  mkcert -client -cert-file "$PROJ_ROOT/tls/client-cert.pem" -key-file "$PROJ_ROOT/tls/client-key.pem" localhost ::1 email@localhost
  cp "$(mkcert -CAROOT)/rootCA.pem" "$PROJ_ROOT/tls/rootCA.pem"
  openssl x509 -noout -text -in "$PROJ_ROOT/tls/server-cert.pem"
  openssl x509 -noout -text -in "$PROJ_ROOT/tls/client-cert.pem"
  openssl pkcs12 -export -out "$PROJ_ROOT/tls/keystore.p12" -inkey "$PROJ_ROOT/tls/client-key.pem" \
    -in "$PROJ_ROOT/tls/client-cert.pem" -password pass:password
  keytool -importkeystore -srcstoretype PKCS12 -srckeystore "$PROJ_ROOT/tls/keystore.p12" \
    -srcstorepass password -destkeystore "$PROJ_ROOT/tls/keystore.jks" -deststorepass password
  keytool -importcert -trustcacerts -file "$PROJ_ROOT/tls/rootCA.pem" \
    -storepass password -noprompt -keystore "$PROJ_ROOT/tls/truststore.jks"
fi


echo -e "\nWriting server configuration $SERVER_CONF_FILENAME:\n"
tee "$SERVER_CONF_FILENAME" <<EOF
server_name: ${SERVERNAME}
client_advertise: "${NATSHOST}:${NATSPORT}"
port: ${NATSPORT}
monitor_port: ${NATSMONITORPORT}

jetstream {
  store_dir: "$PROJ_ROOT/state"
}
EOF

if [ "$TLS" = "true" ]; then
  tee -a "$SERVER_CONF_FILENAME" <<EOF
tls {
  cert_file: "$PROJ_ROOT/tls/server-cert.pem"
  key_file:  "$PROJ_ROOT/tls/server-key.pem"
  ca_file:   "$PROJ_ROOT/tls/rootCA.pem"
  verify:    true
}

EOF
fi

if [ "$PKI" = "true" ]; then
tee -a "$SERVER_CONF_FILENAME" <<EOF
operator: "$PROJ_ROOT/vault/nats/${OPERATORNAME}/${OPERATORNAME}.jwt"
system_account: "${SYSTEMACCTPUBNKEY}"

resolver: {
  type: full
  dir: "./state/.jwt"
  allow_delete: true
  interval: "2m"
  limit: 1000
}
EOF
else
tee -a "$SERVER_CONF_FILENAME" <<EOF
accounts: {
    AcctA: {
	  jetstream: enabled
	  users: [ {user: UserA1, password: s3cr3t}, {user: UserA2, password: s3cr3t} ]
	},
    AcctB: {
	  jetstream: enabled
	  users: [ {user: UserB1, password: s3cr3t}, {user: UserB2, password: s3cr3t} ]
	},
    AcctC: {
	  jetstream: enabled
	  users: [ {user: UserC1, password: s3cr3t}, {user: UserC2, password: s3cr3t} ]
	},
	${SYSTEMACCTNAME}: {
	    users: [ {user: ${SYSTEMUSERNAME}, password: s3cr3t} ]
    }
}
system_account: "${SYSTEMACCTNAME}"
EOF
fi

RUN_SERVER_FILENAME="$PROJ_ROOT/server_bin/${PREFIX}run-server.sh"

if [ "$DOCKER" = "true" ]; then
echo -e "\nWriting server start script (docker):\n"
tee "$RUN_SERVER_FILENAME" <<EOF
#!/bin/bash

docker run -d \\
  --mount type=bind,source="$(pwd)/conf",target=/conf \\
  --mount type=bind,source="$(pwd)/vault",target=/vault \\
  --mount type=bind,source="$(pwd)/state",target=/state \\
  -p ${NATSPORT}:${NATSPORT} \\
  -p ${NATSMONITORPORT}:${NATSMONITORPORT} \\
  nats:latest --config $SERVER_CONF_FILENAME
EOF
else
echo -e "\nWriting server start script:\n"
tee "$RUN_SERVER_FILENAME"  <<EOF
#!/bin/bash

nats-server --config "$SERVER_CONF_FILENAME"
EOF
fi
chmod u+x "$RUN_SERVER_FILENAME"

echo -e "\nWriting env.json file:\n"
tee "$PROJ_ROOT/conf/env.json" <<EOF
{
 "PREFIX": "${PREFIX}",
 "PKI": "${PKI}",
 "TLS": "${TLS}",
 "DOCKER": "${DOCKER}",
 "SERVERNAME": "${SERVERNAME}",
 "NATSHOST": "${NATSHOST}",
 "NATSPORT": "${NATSPORT}",
 "NATSMONITORPORT": "${NATSMONITORPORT}",
 "OPERATORNAME": "${OPERATORNAME}",
 "SYSTEMACCTNAME": "${SYSTEMACCTNAME}",
 "SYSTEMUSERNAME": "${SYSTEMUSERNAME}",
 "NATSURL": "nats://${NATSHOST}:${NATSPORT}",
 "MONITORURL": "http://${NATSHOST}:${NATSMONITORPORT}"
}
EOF
