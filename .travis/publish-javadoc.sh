#!/bin/sh

set -o errexit

#
# Setup git parameters
#
git config --global user.email "travis@travis-ci.org"
git config --global user.name "travis-ci"

# Set git credentials for https clone and push
git config credential.helper "store --file=.git/credentials"
echo "https://${GH_TOKEN}:@github.com" > .git/credentials

# Get to the Travis build directory, configure git and clone the repo
export TARG=${TRAVIS_BUILD_DIR}/target
cd ${TARG}

#
# Clone gh-pages branch
#
git clone --quiet --branch=gh-pages https://github.com/nats-io/jnats.git gh-pages > /dev/null

# Copy javadoc, Commit and Push the Changes
cd gh-pages
git rm -rf .
(cd ${TARG}/apidocs; tar cf - .) | tar xf -
git add -f .
git commit -m "Latest javadoc on successful travis build $TRAVIS_BUILD_NUMBER auto-pushed to gh-pages"
git config -l
git push -fq origin gh-pages > /dev/null
