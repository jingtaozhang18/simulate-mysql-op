#!/bin/bash
set -eu
set -o pipefail

cd /go/src/github.com/jingtaozhang18/codes

export GOPROXY="https://goproxy.io"

go run concurrency_simulate.go $1 $2
