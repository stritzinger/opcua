# Generate certificates

For development we are using certs in x509 v3 format as required by OPCUA.
The keys are all RSA keys since for now we only support RSA-based security policies.

Call this script to generate the certificate binaries.
The script will use config files present in this directory.

    ./scripts/generate_certificates.sh

The script will generate a multi level PKI.
If you want to test the security protocol with third party applications
 make sure you install all CAs in their PKI.

## Root CA

The root CA cert is as expected self signed and defined in `CA.cfg`

    openssl genrsa -out CA.key 4096
    openssl req -new -x509 -config CA.cfg -key CA.key -out CA.pem

## ICA and End Entity certs

Issued certificates are all generated in the same way.
They only differ in few attributes specified in the `.cfg` files.
Starting from a key, we generate a CSR (req option) taking client.cfg as argument.
Then we generate a cert signed by the CA or an ICA and featuring extensions from `*_v3_ext.cfg`.

    openssl genrsa -out client.key 2048
    openssl req -new -x509 -config client.cfg -key client.key -out client.csr
    openssl x509 -req -extfile client_v3_ext.cfg
            -in client.csr -CA CA.pem
            -CAkey CA.key -CAcreateserial -out client.pem

## Utils

To check if the cert looks good

    openssl x509 -in cert.pem -text

To check if the cert request looks good

    openssl req -in cert.csr -text

To generate DER from a PEM file, use:

    openssl x509 -in cert.pem -out cert.der -outform DER
