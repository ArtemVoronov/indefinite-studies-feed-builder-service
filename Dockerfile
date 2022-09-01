FROM golang:1.18

RUN mkdir /app
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . ./

RUN go build -o ./indefinite-studies-feed-builder-service

CMD [ "./indefinite-studies-feed-builder-service" ]