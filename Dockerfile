FROM python:3.8

WORKDIR /app
copy pipeline_1.py pipeline_c.py

RUN pip install pandas

ENTRYPOINT [ "bash" ]