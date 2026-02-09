FROM tailscale/tailscale:v1.80.3 AS tailscale

FROM rancher/k3s:v1.34.3-k3s1

USER root

COPY --from=tailscale /usr/local/bin/tailscaled /usr/local/bin/tailscaled
COPY --from=tailscale /usr/local/bin/tailscale /usr/local/bin/tailscale
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

RUN chmod +x /usr/local/bin/entrypoint.sh && \
    mkdir -p /run/tailscale /var/lib/tailscale /var/lib/rancher/k3s

VOLUME ["/var/lib/tailscale", "/var/lib/rancher/k3s"]

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
