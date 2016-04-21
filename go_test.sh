#!/bin/bash

export LIVERPOOL_ENV=test
go test -v $(go list ./... | grep -v vendor)      
