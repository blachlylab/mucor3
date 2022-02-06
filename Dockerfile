FROM blachlylab/dlang-htslib-static

ADD . /home/mucor3
WORKDIR /home/mucor3
RUN make STATIC=1