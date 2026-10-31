FROM node:24.22.0-slim AS node
FROM golang:1.27.3
COPY --from=node /usr/local/bin/node /usr/local/bin/node
ENV GOSUMDB="sum.golang.org https://sum.golang.org"
