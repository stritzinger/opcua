# Generate self-signed keys and certificates

For development we are using self-signed certs in x509 v3 format.
The certificates follow respective configurations for server and client.

This script will generate the binaries and place them in `priv/_certificates`

    ./generate.sh

## Client self-signed

    openssl genrsa -out client.key 2048
    openssl req -new -x509 -config client.cfg -key client.key -out client.pem

## Server self-signed

    openssl genrsa -out server.key 2048
    openssl req -new -x509 -config server.cfg -key server.key -out server.pem

## Utils

To check if the cert looks good

    openssl x509 -in cert.pem -text

To generate DER from a PEM file, use:

    openssl x509 -in cert.pem -out cert.der -outform DER
