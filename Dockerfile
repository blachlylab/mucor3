FROM continuumio/miniconda3 AS base
MAINTAINER Charles Gregory <charles.gregory@osumc.edu>
# install system packages
RUN apt-get update
RUN apt-get install -y jq
RUN apt-get install -y golang
RUN apt-get install -y make

# install python packages
RUN conda install -y pandas
RUN conda install -y flask
RUN conda install -y -c conda-forge celery

#add mucor3 files
COPY . /home/user/mucor3/
WORKDIR /home/user/mucor3/

FROM base as flask
WORKDIR /home/user/mucor3/mucor3-flask/
ENTRYPOINT ["python"]
CMD ["app.py"]

FROM base as celery_worker
#build vcf_atomizer
ENV GOPATH /home/user/mucor3/go
WORKDIR /home/user/mucor3/vcf_atomizer
RUN make deps
RUN make build
WORKDIR /home/user/mucor3/mucor3-flask/
#ENV FLASK_APP app.py
ENTRYPOINT celery -A celery_server worker --concurrency=20 --loglevel=info
