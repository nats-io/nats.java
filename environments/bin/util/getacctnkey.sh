#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

source $SCRIPT_DIR/../setnscenv.sh

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

OPERATORNAME=`cat $SCRIPT_DIR/../conf/env.json | jq -r .OPERATORNAME`

ACCTNAME=$1

getpubnkey () {
  echo `cat ${NSC_HOME}/nats/${OPERATORNAME}/accounts/${ACCTNAME}/${ACCTNAME}.jwt | $SCRIPT_DIR/decodejwt.sh | jq -r '.["sub"] | select( . != null )'`
}

getpubnkey

