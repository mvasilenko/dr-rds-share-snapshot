FROM alpine:3.10
RUN apk add --update python3 && pip3 install boto3 awscli

COPY *.py docker-entrypoint.sh /

ENV PYTHONUNBUFFERED=1
CMD [ "./docker-entrypoint.sh" ]
