FROM centos

ADD deps/mongodb-linux-x86_64-3.6.4.tgz /usr/local/
ADD deps/python-centos7-x86_64-3.6.5.tgz /usr/local/

RUN yum -y install zlib-devel openssl-devel

RUN mv /usr/local/mongodb-linux-x86_64-3.6.4 /usr/local/mongodb \
 && ln -s /usr/local/mongodb/bin/mongod /usr/bin/mongod \
 && ln -s /usr/local/mongodb/bin/mongo /usr/bin/mongo \
 && mkdir -p /data/db \
 && mkdir -p /data/log \
 && touch /data/log/log-file

RUN ln -s /usr/local/python3/bin/python3 /usr/bin/python3 \
 && ln -s /usr/local/python3/bin/pip3 /usr/bin/pip3


CMD mongod --dbpath=/data/db --logpath=/data/log/log-file


