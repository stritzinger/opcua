#!/bin/bash
DST=../priv/certificates/
mkdir ${DST}
# Client
openssl genrsa -out ${DST}client.key 2048
openssl req -new -x509 -config client.cfg -key ${DST}client.key -out ${DST}client.pem
# Server
openssl genrsa -out ${DST}server.key 2048
openssl req -new -x509 -config server.cfg -key ${DST}server.key -out ${DST}server.pem