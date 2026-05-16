FROM golang:1.22 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o collector .

FROM debian:bookworm-slim
COPY --from=builder /app/collector /collector
RUN mkdir -p /data
EXPOSE 8815 9090
CMD ["/collector"]