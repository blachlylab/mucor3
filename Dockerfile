FROM blachlylab/dlang-htslib-static

ADD . /home/mucor3
WORKDIR /home/mucor3/mucor3
RUN dub build --compiler ldc2 -c static-alpine -b release