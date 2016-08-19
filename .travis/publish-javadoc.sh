#!/bin/sh

set -o errexit

eval "$(ssh-agent -s)" #start the ssh agent
chmod 600 .travis/keyrings/deploy_key.pem # this key should have push access
ssh-add .travis/keyrings/deploy_key.pem

#
# Setup git parameters
#
git config --global user.email "travis@travis-ci.org"
git config --global user.name "travis-ci"


# Get to the Travis build directory, configure git and clone the repo
export TARG=${TRAVIS_BUILD_DIR}/target
cd ${TARG}

#
# Clone gh-pages branch
#
echo Executing 'git clone --quiet --branch=gh-pages https://github.com/nats-io/jnats.git gh-pages > /dev/null'
git clone --quiet --branch=gh-pages https://github.com/nats-io/jnats.git gh-pages > /dev/null

# Copy javadoc, Commit and Push the Changes
cd gh-pages

git rm -rf .
(cd ${TARG}/apidocs; tar cf - .) | tar xf -
git add -f .
git commit -m "Latest javadoc on successful travis build $TRAVIS_BUILD_NUMBER auto-pushed to gh-pages"

git remote add pages ssh://git@github.com/nats-io/jnats.git
git push -fq pages gh-pages > /dev/null

echo "Latest javadoc on successful travis build $TRAVIS_BUILD_NUMBER auto-pushed to gh-pages"
