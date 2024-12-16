ARG AIRFLOW_IMAGE_NAME="apache/airflow:2.9.0"
FROM ${AIRFLOW_IMAGE_NAME}
USER root
RUN apt update && apt install -y \ 
    libgl1-mesa-glx \
    curl  \
    ranger \
    make \
    libglib2.0-0 && apt-get clean  \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install nltk && python -m nltk.downloader stopwords && python -m nltk.downloader popular
ADD requirements.txt /opt/airflow/requirements.txt
RUN pip install -r /opt/airflow/requirements.txt  --no-cache-dir