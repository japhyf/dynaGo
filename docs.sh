#!/usr/bin/env bash
docker run --rm -v $(pwd):/go/src/app -p 6060:6060 --name go_doc -d golang:latest godoc -http=:6060
