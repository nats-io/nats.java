# source me from the project root...

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

export PROJ_ROOT="$SCRIPT_DIR/environments"
export PROJ_BIN="$PROJ_ROOT/bin"
export PATH="$PROJ_BIN:$PATH"

export NKEYS_PATH=${PROJ_ROOT}/vault/.nkeys
export NSC_HOME=${PROJ_ROOT}/vault

export TLS=`cat ${PROJ_ROOT}/conf/env.json | jq -r .TLS`



if [ "$TLS" = "true" ]; then
  export NATS_CA=${PROJ_ROOT}/tls/rootCA.pem
  export NATS_KEY=${PROJ_ROOT}/tls/client-key.pem
  export NATS_CERT=${PROJ_ROOT}/tls/client-cert.pem
fi

