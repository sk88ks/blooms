.PHONEY: all setup test cover

all: setup cover

setup:
		go get golang.org/x/tools/cmd/cover
		go get github.com/smartystreets/goconvey
		go get github.com/axw/gocov/gocov
		go get github.com/mattn/goveralls      
		go get ./...

test:
		./go_test.sh

cover:
		go test -v -coverprofile=coverage.txt -covermode=count ./
