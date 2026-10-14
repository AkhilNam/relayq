FROM node:22-alpine AS ui
WORKDIR /src/web
COPY web/package*.json ./
RUN npm ci
COPY web/ ./
RUN npm run build
FROM golang:1.24-alpine AS backend
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /relayq ./cmd/relayq
FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D app
WORKDIR /app
COPY --from=backend /relayq /app/relayq
COPY --from=ui /src/web/dist /app/web/dist
USER app
EXPOSE 8080
CMD ["/app/relayq"]
