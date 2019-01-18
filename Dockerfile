FROM golang:1.11.4-alpine

WORKDIR /go/src/github.com/rking788/bungie-manifest-updater

RUN apk add --update gcc musl-dev

COPY . .

RUN go get .

RUN GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /bin/app 
