FROM python:3
MAINTAINER georges.alkhouri@gmail.com

ENV INPUT_FILE /shared/source.csv
ENV OUTPUT_DIR /shared

VOLUME /shared
COPY transformation.py /
COPY run.sh /usr/local/bin

RUN pip install pandas rdflib python-slugify

CMD ["run.sh"]
