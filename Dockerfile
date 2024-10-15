# Use uma imagem base oficial do Python
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH /app

RUN apt-get update \
    && apt-get install -y cron \
    && apt-get autoremove -y
RUN pip3 install -U pip setuptools && pip3 install pipenv

RUN mkdir /app
COPY . /app/

WORKDIR /app

RUN pipenv install --deploy --system

# Defina o comando para iniciar o aplicativo
CMD ["python", "main.py"]
