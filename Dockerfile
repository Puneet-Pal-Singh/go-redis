FROM golang:1.22.4 AS builder

WORKDIR /app

COPY go.mod ./

RUN go mod download

COPY . .

# RUN go build -o main .
# FROM alpine:latest
# WORKDIR /app

# Build the Go app statically
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .

# Start a new stage from scratch
FROM scratch

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /app/main .

EXPOSE 6378

CMD ["./main"]