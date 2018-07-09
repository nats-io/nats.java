#!/usr/bin/env bash
if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
openssl aes-256-cbc -K $encrypted_f07928735f08_key -iv $encrypted_f07928735f08_iv -in .travis/nats.travis.gpg.enc -out .travis/nats.travis.gpg -d
./gradlew uploadArchives -PossrhUsername=${SONATYPE_USERNAME} -PossrhPassword=${SONATYPE_PASSWORD} -Psigning.keyId=${GPG_KEY_ID} -Psigning.password=${GPG_KEY_PASSPHRASE} -Psigning.secretKeyRingFile=.travis/nats.travis.gpg
fi