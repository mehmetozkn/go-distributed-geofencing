# ── Build stage ─────────────────────────────────
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git

ENV GOTOOLCHAIN=auto

WORKDIR /app

COPY go.mod go.sum ./
RUN GOTOOLCHAIN=auto go mod download

COPY . .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/server ./cmd/server

# ── Run stage ──────────────────────────────────
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata tini

COPY --from=builder /bin/server /bin/server

EXPOSE 8080

ENTRYPOINT ["tini", "--"]
CMD ["/bin/server"]
