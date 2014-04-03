all: gomarquise

gomarquise:
	go get
	go get -t
	go build
	go test -race
	go install
