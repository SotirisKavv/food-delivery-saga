FROM golang:1.23.9-alpine AS builder

WORKDIR /app

# Copy go mod files and download deps with cache
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	go mod download

# Copy source code
COPY . .

# Build the application with cache mounts
RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	CGO_ENABLED=0 GOOS=linux go build -o payment ./cmd/payment

# Final stage
FROM scratch

# Copy the binary from builder
COPY --from=builder /app/payment .

# Run the binary
CMD ["./payment"]