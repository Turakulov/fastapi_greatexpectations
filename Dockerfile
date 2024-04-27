FROM python:3.10 AS base
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends gnupg2 curl \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/apt/lists/* \
    && rm -rf /var/cache/apt/*


# set work directory
WORKDIR /code

# set env variables
ENV PYTHONPATH=/home/code/ PYTHONHASHSEED=0
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# copy project
COPY . .

