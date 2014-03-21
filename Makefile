all: gomarquise

gomarquise: testdeps
	go get
	go build
#	go test -race
	go install

testdeps:
	go get github.com/pebbe/zmq4
