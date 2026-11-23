FROM node:22-alpine AS styles
WORKDIR /src
COPY package*.json ./
RUN npm ci
COPY tailwind.config.cjs ./
COPY web ./web
RUN npm run build:css

FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=styles /src/web/style.css ./web/style.css
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /maxstore ./cmd/maxstore

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && addgroup -g 10001 maxstore && adduser -D -u 10001 -G maxstore maxstore && mkdir /data && chown maxstore:maxstore /data
USER maxstore
WORKDIR /app
COPY --from=build /maxstore /usr/local/bin/maxstore
ENV DATA_DIR=/data HTTP_ADDR=0.0.0.0:8080 S3_ADDR=0.0.0.0:9000
EXPOSE 8080 9000
VOLUME /data
ENTRYPOINT ["maxstore"]
