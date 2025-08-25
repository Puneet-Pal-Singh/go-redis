# Dockerfile

# Stage 1: Build the application
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy dependency files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the binary, statically linking it and disabling CGO for a small, portable binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /go/bin/redis-server ./cmd/redis-server/

# Stage 2: Create the final, small image
FROM alpine:latest

# Create a dedicated directory for the app's data
RUN mkdir /data

# Copy the built binary from the builder stage
COPY --from=builder /go/bin/redis-server /go/bin/redis-server

# Expose the Redis port and the data volume
EXPOSE 6378
VOLUME /data

# Set the command to run when the container starts
# The default dbpath will be /data/data.rdb
CMD ["/go/bin/redis-server", "--dbpath=/data/data.rdb"]