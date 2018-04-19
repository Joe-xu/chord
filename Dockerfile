FROM golang:1.10

MAINTAINER Joe Xu "joe.0x01@gmail.com"

WORKDIR /root

# RUN apt-get update && \
#     apt-get -y install zip

#install protobuf
# RUN wget https://github.com/google/protobuf/releases/download/v3.3.0/protoc-3.3.0-linux-x86_64.zip && \
#     unzip protoc-3.3.0-linux-x86_64.zip -d /usr/local/protoc && \
#     rm protoc-3.3.0-linux-x86_64.zip
# ENV PATH=$PATH:/usr/local/protoc/bin

# ENV GOPATH=/gopath 
# ENV PATH=$PATH:$GOPATH/bin

# WORKDIR $GOPATH/src
# VOLUME $GOPATH

COPY *.go /chord/
COPY ./test/*.go /chord/test
RUN cd /chord/ && go get -v 
RUN go build /chord/test

CMD  ["/bin/bash"]