FROM puckel/docker-airflow:1.10.9

RUN pip install -U pip setuptools wheel

RUN pip install bs4 \
    && pip install requests \
    && pip install -U spacy

RUN python -m spacy download en_core_web_sm