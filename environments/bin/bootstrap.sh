"$PROJ_BIN/bootstrap-jetstream.sh" -t false -p true -f pki_notls -o 4222
"$PROJ_BIN/bootstrap-jetstream.sh" -t true -p false -f tls_only -o 4222
"$PROJ_BIN/bootstrap-jetstream.sh" -t true -p true -f pki_tls -o 4222

