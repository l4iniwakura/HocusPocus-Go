FROM golang:1.27.3 AS build
WORKDIR /src
ENV GOSUMDB="sum.golang.org https://sum.golang.org"
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /hocuspocus ./cmd/hocuspocus

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /hocuspocus /hocuspocus
USER 65532:65532
EXPOSE 1234
ENTRYPOINT ["/hocuspocus"]
CMD ["-listen", "0.0.0.0:1234"]
