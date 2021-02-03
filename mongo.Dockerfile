FROM mongo:4.4

COPY file.key /opt/keyfile
RUN chmod 400 /opt/keyfile && chown 999:999 /opt/keyfile
