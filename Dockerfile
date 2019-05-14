FROM tiangolo/meinheld-gunicorn-flask:python3.7 AS base
MAINTAINER Charles Gregory <charles.gregory@osumc.edu>
# install system packages
RUN apt-get update
RUN apt-get install -y jq
RUN apt-get install -y golang
RUN apt-get install -y make
RUN apt-get install -y zip
RUN apt-get install -y curl

# install python packages
RUN pip install pandas
RUN pip install flask
RUN pip install celery

#add mucor3 files
COPY . /app


FROM base as flask

FROM base as celery_worker
#build vcf_atomizer
ENV GOPATH /home/user/mucor3/go
WORKDIR /app/vcf_atomizer
RUN make deps
RUN make build
WORKDIR /app/
#ENV FLASK_APP app.py
ENTRYPOINT celery -A mucor3_flask.celery_server worker --concurrency=20 --loglevel=info
