FROM tiangolo/meinheld-gunicorn-flask:python3.7 AS base
MAINTAINER Charles Gregory <charles.gregory@osumc.edu>
# install system packages
RUN apt-get update
RUN apt-get install -y jq
RUN apt-get install -y golang
RUN apt-get install -y make
RUN apt-get install -y zip
RUN apt-get install -y curl
RUN apt-get install -y gcc
RUN apt-get install -y make
RUN apt-get install -y libbz2-dev
RUN apt-get install -y zlib1g-dev
RUN apt-get install -y libncurses5-dev 
RUN apt-get install -y libncursesw5-dev
RUN apt-get install -y liblzma-dev
RUN apt-get install -y tar

WORKDIR /htslib
RUN wget https://github.com/samtools/htslib/releases/download/1.9/htslib-1.9.tar.bz2
RUN tar -vxjf htslib-1.9.tar.bz2
WORKDIR /htslib/htslib-1.9
RUN make
RUN make prefix=/usr install

RUN curl -fsS https://dlang.org/install.sh | bash -s ldc

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
WORKDIR /app/depthGauge
ENV LIBRARY_PATH /htslib/htslib-1.9/
RUN /bin/bash -c "source ~/dlang/ldc-1.15.0/activate;LIBRARY_PATH=/htslib/htslib-1.9/;dub build --build release"
WORKDIR /app/
#ENV FLASK_APP app.py
ENTRYPOINT celery -A mucor3_flask.celery_server worker --concurrency=20 --loglevel=info
