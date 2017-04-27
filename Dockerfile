FROM golang:alpine

RUN apk update && apk add git

WORKDIR /go/src/esub
COPY *.go /go/src/esub/

RUN go get -d -v
RUN go install -v .

CMD esub
