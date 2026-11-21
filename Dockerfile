FROM debian:bookworm-slim

ARG TARGETARCH
ARG TARGETVARIANT

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --gid 1000 speedtest && \
    useradd --uid 1000 --gid 1000 --no-create-home --shell /usr/sbin/nologin speedtest

COPY --chmod=0755 dist/${TARGETARCH}${TARGETVARIANT}/cloudflare-speedtest-exporter /usr/local/bin/cloudflare-speedtest-exporter

USER 1000:1000

EXPOSE 9798

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/cloudflare-speedtest-exporter", "--healthcheck"]

ENTRYPOINT ["/usr/local/bin/cloudflare-speedtest-exporter"]
