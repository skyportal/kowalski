FROM mongo:6.0

COPY mongo_key.yaml /opt/keyfile
RUN chmod 400 /opt/keyfile && chown 999:999 /opt/keyfile
