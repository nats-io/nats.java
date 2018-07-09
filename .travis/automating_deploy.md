# Automating the travis deploy

See https://rishikeshdarandale.github.io/build/deploying-to-oss-sonatype-using-travis/

The global/secure values are used to deploy, they can be regenerated/created by someone with access to
the sonatype repository

You can use --add or copy and paste to the global section manually.

> cd java-nats
> travis encrypt SONATYPE_USERNAME="<YOUR_JIRA_USER_NAME>"
> travis encrypt SONATYPE_PASSWORD="<YOUR_JIRA_PASSWORD>"
> travis encrypt GPG_KEY_ID="<TRAVIS_GPG_KEY_ID>"
> travis encrypt GPG_KEY_PASSPHRASE="<TRAVIS_KEY_PASSPHRASE>"

or use

> travis encrypt -i

to avoid passwords in shell history.

To get the signing key to travis, export it and put it in the .travis folder

> gpg --export-secret-key <TRAVIS_KEY_ID> > nats.travis.gpg
> travis encrypt-file .travis/nats.travis.gpg

Update the before install as instructed