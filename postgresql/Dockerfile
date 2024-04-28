# pull official base image
FROM postgres:15.2-bullseye

WORKDIR /fastapi_ge
# run create.sql on init
ADD create.sql /docker-entrypoint-initdb.d


