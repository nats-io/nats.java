#!/bin/bash

# https://gist.github.com/angelo-v/e0208a18d455e2e6ea3c40ad637aac53

# Pipe the JWT stdin to this script...

# pad base64URL encoded to base64
paddit() {
  input=$1
  l=`echo -n $input | wc -c`
  while [ `expr $l % 4` -ne 0 ]
  do
    input="${input}="
    l=`echo -n $input | wc -c`
  done
  echo $input
}

# read and split the token and do some base64URL translation
read jwt
read h p s <<< $(echo $jwt | tr [-_] [+/] | sed 's/\./ /g')

h=`paddit $h`
p=`paddit $p`

# assuming we have jq installed
echo $h | base64 -d | jq
echo $p | base64 -d | jq
