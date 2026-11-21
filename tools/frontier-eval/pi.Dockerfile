FROM node:22.23.5-bookworm-slim@sha256:e9f565d32859f0cbfb52125eafe8be39390f24dac302dc3cdf566e8ba3ab21ee AS pi
ADD --checksum=sha256:da642dbff51d9afc729d67f8aa327decaf16e8b4efee792f0be2da817bd6760e https://codeload.github.com/nvm-sh/nvm/tar.gz/refs/tags/v0.40.2 /tmp/nvm.tgz
RUN npm install --global --ignore-scripts --no-audit --no-fund @earendil-works/pi-coding-agent@1.4.0 \
    && test "$(pi --version)" = 1.4.0 \
    && mkdir -p /nvm/alias /nvm/versions/node/v22.23.5 \
    && tar -C /nvm -xzf /tmp/nvm.tgz --strip-components=1 nvm-0.40.2/nvm.sh nvm-0.40.2/nvm-exec \
    && echo 22 > /nvm/alias/default \
    && cp -a /usr/local/bin /usr/local/lib /usr/local/include /nvm/versions/node/v22.23.5/ \
    && tar -C /nvm -czf /pi.tgz nvm.sh nvm-exec alias versions

FROM scratch
COPY --from=pi /pi.tgz /
