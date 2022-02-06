FROM charesgregory/dlang-ldc-static

ADD . /home/mucor3
WORKDIR /home/mucor3
RUN make STATIC=1