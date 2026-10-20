ARG GO_VERSION=1.26.8
FROM golang:${GO_VERSION}-alpine AS build
ARG GOPROXY=https://proxy.golang.org|https://goproxy.cn|direct
ENV GOPROXY=${GOPROXY}
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/taskflow ./cmd/server \
    && CGO_ENABLED=0 GOOS=linux go build -o /out/executor-worker ./cmd/executor-worker \
    && CGO_ENABLED=0 GOOS=linux go build -o /out/schema-migrate ./cmd/schema-migrate \
    && CGO_ENABLED=0 GOOS=linux go build -o /out/taskflowctl ./cmd/taskflowctl \
    && CGO_ENABLED=0 GOOS=linux go build -o /out/ai-phase7-client ./scripts/ai-phase7-client.go \
    && CGO_ENABLED=0 GOOS=linux go build -o /out/ai-phase10-client ./scripts/ai-phase10-client \
    && CGO_ENABLED=0 GOOS=linux go build -o /out/release-s3-tool ./scripts/release-s3-tool

FROM alpine:3.24.1
ARG VERSION=dev
ARG REVISION=unknown
ARG SOURCE=https://github.com/yfj898/taskflow-go
LABEL org.opencontainers.image.title="taskflow-go" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.source="${SOURCE}" \
      org.opencontainers.image.licenses="Apache-2.0"
RUN adduser -D -u 10001 app \
    && mkdir -p /var/lib/taskflow/dlq-archive /var/lib/taskflow/dlq-archive-replica \
    && chown -R app:app /var/lib/taskflow
USER 10001
COPY --from=build /out/taskflow /taskflow
COPY --from=build /out/executor-worker /executor-worker
COPY --from=build /out/schema-migrate /schema-migrate
COPY --from=build /out/taskflowctl /taskflowctl
COPY --from=build /out/ai-phase10-client /ai-phase10-client
EXPOSE 8080 9091 9093
CMD ["/taskflow"]
