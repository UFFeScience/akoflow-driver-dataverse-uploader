FROM python:3.11-alpine

WORKDIR /app

COPY script.py dataset.sample.json /app/

RUN pip install --no-cache-dir pyDataverse

ENV DATAVERSE_BASE_URL=https://demo.dataverse.org \
    DATAVERSE_PARENT_ALIAS=dataverselucas \
    DATA_ROOT=/data

ENTRYPOINT ["/bin/sh","-c"]
CMD ["python /app/script.py"]
