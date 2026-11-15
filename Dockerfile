# Multi-stage build: static web UI -> Go binary -> small runtime image.
# Used by `fly deploy` (Fly.io builds it remotely; no local Docker needed).

FROM node:22-alpine AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-fund --no-audit
COPY web/ ./
RUN npm run build

FROM golang:1.27-alpine AS server
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /raftkv ./cmd/raftkv

FROM alpine:3.20
WORKDIR /app
COPY --from=server /raftkv /app/raftkv
COPY --from=web /web/out /app/web/out
EXPOSE 8080
# -fresh: a demo deployment starts from an empty cluster on every boot.
CMD ["/app/raftkv", "-addr", ":8080", "-data", "/tmp/raft-data", "-fresh", "-scale", "20"]
