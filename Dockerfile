# syntax=docker/dockerfile:1

FROM docker.io/library/python:3.12-alpine

ARG TARGETARCH
ARG VERSION

USER root
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && rm -rf /tmp/*

COPY src/ src/

RUN chown -R nobody:nogroup /app \
    && chmod -R 755 /app

USER nobody:nogroup

ENTRYPOINT ["python", "-m", "src.main"]
