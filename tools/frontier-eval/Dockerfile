# Build with: docker build -t frontier-eval:v1.1.0 - < Dockerfile
# Local Docker Engine, from a writable directory shared with the daemon:
#   work=$(pwd -P)
#   docker run --rm -v /var/run/docker.sock:/var/run/docker.sock \
#     -v "$work:$work" -w "$work" -e HOME="$work/.harbor-home" \
#     -e DOCKER_DEFAULT_PLATFORM=linux/amd64 frontier-eval:v1.1.0 \
#     run -e docker -d frontier-harness-eval/frontier-harness-eval@v1.1.0 \
#     -i '*/fix-git' -a oracle -k 1 --override-storage-mb 32768
# Only the Harbor controller needs the socket. Harbor starts a fresh task container per trial.
FROM docker:29.10.0-cli@sha256:34507eb5069d8be82c95dd3b6987d4250fd5fee49b82e2509b7d7a7030740d6d AS docker-cli
FROM ghcr.io/astral-sh/uv:0.13.9-python3.14-trixie-slim@sha256:da0f1f04404173d7d2fa158112e09624d5a80262efa86c89882b68389dfd44eb

COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker-cli /usr/local/libexec/docker/cli-plugins/ /usr/local/libexec/docker/cli-plugins/

WORKDIR /opt/frontier-eval
ADD --checksum=sha256:0b143fe4cc565b6da44687ef2d77b339038a28ee2253612679728801b70d9482 https://codeload.github.com/frontier-harness-eval/eval/tar.gz/84a29789534af5b316df642be279c23c84caa2e4 /tmp/eval.tar.gz
RUN tar -xzf /tmp/eval.tar.gz --strip-components=1 && rm /tmp/eval.tar.gz \
    && uv sync --locked --no-dev --no-editable --no-cache --no-python-downloads \
    && .venv/bin/harbor --version && docker compose version && docker buildx version

ENV PATH="/opt/frontier-eval/.venv/bin:$PATH"
WORKDIR /work
ENTRYPOINT ["harbor"]
CMD ["--help"]
