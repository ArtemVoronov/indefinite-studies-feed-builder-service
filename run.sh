#!/bin/sh

down() {
    docker-compose down
}

purge() {
    docker volume rm indefinite-studies-feed-builder-service_mongo-volume
}  

build() {
    docker-compose build api
}

start() {
    docker-compose up -d
}

tail() {
    docker-compose logs -f
}

case "$1" in
  start)
    down
    purge
    build
    start
    tail
    ;;
  stop)
    down
    ;;
  tail)
    tail
    ;;
  purge)
    down
    purge
    ;;
  *)
    echo "Usage: $0 {start|stop|purge|tail}"
esac