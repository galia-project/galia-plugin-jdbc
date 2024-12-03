#!/bin/bash

GITHUB_PAT=$1

git clone https://$GITHUB_PAT@github.com/galia-project/galia.git \
    && cd galia \
    && mvn install -DskipTests -Ddependency-check.skip=true \
    && cd .. \
    && mvn --quiet dependency:resolve

mvn --batch-mode test
