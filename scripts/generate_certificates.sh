#!/bin/bash
DST=priv/certificates/
CFG=certificates/



mkdir ${DST}
rm -rf $DST}*

# # Clients ICA
# openssl genrsa -out ${DST}clients_ICA.key 3072
# openssl req -new -config ${CFG}clients_ICA.cfg -key ${DST}clients_ICA.key -out ${DST}clients_ICA.csr
# openssl x509 -req -extfile ${CFG}clients_ICA_v3_ext.cfg \
#     -in ${DST}clients_ICA.csr -CA  ${DST}CA.pem -CAkey ${DST}CA.key \
#     -CAcreateserial -out ${DST}clients_ICA.pem
# # Servers ICA
# openssl genrsa -out ${DST}servers_ICA.key 3072
# openssl req -new -config ${CFG}servers_ICA.cfg -key ${DST}servers_ICA.key -out ${DST}servers_ICA.csr
# openssl x509 -req -extfile ${CFG}servers_ICA_v3_ext.cfg \
#     -in ${DST}servers_ICA.csr -CA  ${DST}CA.pem -CAkey ${DST}CA.key \
#     -CAcreateserial -out ${DST}servers_ICA.pem
# # Client
# openssl genrsa -out ${DST}client.key 2048
# openssl req -new -config ${CFG}client.cfg -key ${DST}client.key -out ${DST}client.csr
# openssl x509 -req -extfile ${CFG}client_v3_ext.cfg \
#     -in ${DST}client.csr -CA  ${DST}clients_ICA.pem -CAkey ${DST}clients_ICA.key \
#     -CAcreateserial -out ${DST}client.pem
# # Server
# openssl genrsa -out ${DST}server.key 2048
# openssl req -new -config ${CFG}server.cfg -key ${DST}server.key -out ${DST}server.csr
# openssl x509 -req -extfile ${CFG}server_v3_ext.cfg \
#     -in ${DST}server.csr -CA ${DST}servers_ICA.pem -CAkey ${DST}servers_ICA.key \
#     -CAcreateserial -out ${DST}server.pem


# CA self signed
openssl genrsa -out ${DST}CA.key 4096
openssl req -new -x509 -config ${CFG}CA.cfg -key ${DST}CA.key -out ${DST}CA.pem

gen_issued_cert()
{
    NAME=$1
    KEY_LEN=$2
    ISSUER=$3
    openssl genrsa -out ${DST}${NAME}.key 2048
    openssl req -new -config ${CFG}${NAME}.cfg -key ${DST}${NAME}.key -out ${DST}${NAME}.csr
    openssl x509 -req -extfile ${CFG}${NAME}_v3_ext.cfg \
        -in ${DST}${NAME}.csr -CA ${DST}${ISSUER}.pem -CAkey ${DST}${ISSUER}.key \
        -CAcreateserial -out ${DST}${NAME}.pem
}
# ICA
gen_issued_cert servers_ICA 3072 CA
gen_issued_cert clients_ICA 3072 CA
# End Entities
gen_issued_cert server 2048 servers_ICA
gen_issued_cert client 2048 clients_ICA

save_in_der()
{
    openssl x509 -in ${DST}$1.pem -outform DER -out ${DST}$1.der
}

# DER certs to copy where is necessary
save_in_der CA
save_in_der clients_ICA
save_in_der servers_ICA