#!/bin/bash

export GOPATH=$PWD
export PATH=$GOPATH/go/bin:$GOPATH/bin:$PATH
mkdir -p $GOPATH/go/bin
mkdir -p $GOPATH/src
mkdir -p $GOPATH/bin
mkdir -p $GOPATH/pkg
