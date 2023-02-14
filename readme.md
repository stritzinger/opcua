
<p align="center">
   <img width="50%" src="doc/OPC-UA-Logo-Color_Large.png">
</p>

# OPC UA

A native Erlang implementation of the OPCUA Binary Protocol.

We use erlang 25 or develop and test this application.

[![GitHub](https://github.com/stritzinger/opcua/workflows/opcua/badge.svg)](https://github.com/stritzinger/opcua/actions)
[![License](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/stritzinger/opcua/blob/master/LICENSE)
[![Version](https://img.shields.io/github/tag/stritzinger/opcua.svg?color=red&label=version)](https://github.com/sstritzinger/opcua/releases)

---
## Quick Start

```sh
# clone
git clone https://github.com/stritzinger/opcua.git
cd opcua

# you can generate the certificates with
./scripts/generate_certificates.sh

# optionally update the nodesets
./scripts/update_nodeset
```
You can quickly try it with the erlang shell:
```sh
# run default server
rebar3 as server shell

# run default client
rebar3 as client shell
```

Read the [Wiki](https://github.com/stritzinger/opcua/wiki) to learn about the available command line API.

---

## Client-Server Features
We base our development on release v1.04 of the OPCUA specification.

Striked checkboxes are not planned.
Empty ones will be added in future.

### Encoding

- [x] OPC UA Binary
- [ ] ~~OPC UA JSON~~
- [ ] ~~OPC UA XML~~

### Transport

- [x] OPC UA TCP (`opc.tcp`)
- [ ] ~~OPC UA HTTPS~~
- [ ] ~~OPC UA XML~~
- [ ] ~~WebSockets~~

### Security

#### Modes

- [x] none
- [x] sign
- [x] sign_and_encypt

#### Policies

- [x] Basic256Sha256
- [x] Aes128-Sha256-RsaOaep

#### Authentication

- [x] Anonymous
- [x] User Name Password
- [ ] X509 Certificate

#### Certificate Validation

- [x] x509 validation (signature, validity ecc ...)
- [ ] Hostname & Application Uri ecc ...
- [ ] Certificate Revocation Lists (CRL)

### Services

The current sets of supported services.

| Service Set                 | Service                       | Supported | Notes        |
|-----------------------------|-------------------------------|-----------|--------------|
| Discovery Service Set       | FindServers                   |           |              |
|                             | FindServersOnNetwork          |           |              |
|                             | GetEndpoints                  | Yes       |              |
|                             | RegisterServer                |           |              |
|                             | RegisterServer2               |           |              |
| Secure Channel Service Set  | OpenSecureChannel             | Yes       |              |
|                             | CloseSecureChannel            | Yes       |              |
| Session Service Set         | CreateSession                 | Yes       |              |
|                             | CloseSession                  | Yes       |              |
|                             | ActivateSession               | Yes       |              |
|                             | Cancel                        |           |              |
| Node Management Service Set | AddNodes                      |           |              |
|                             | AddReferences                 |           |              |
|                             | DeleteNodes                   |           |              |
|                             | DeleteReferences              |           |              |
| View Service Set            | Browse                        | Yes       |              |
|                             | BrowseNext                    |           |              |
|                             | TranslateBrowsePathsToNodeIds |           |              |
|                             | RegisterNodes                 |           |              |
|                             | UnregisterNodes               |           |              |
| Query Service Set           | QueryFirst                    |           |              |
|                             | QueryNext                     |           |              |
| Attribute Service Set       | Read                          | Yes       |              |
|                             | Write                         | Yes       |              |
|                             | HistoryRead                   |           |              |
|                             | HistoryUpdate                 |           |              |
| Method Service Set          | Call                          |           |              |
| MonitoredItems Service Set  | CreateMonitoredItems          |           |              |
|                             | DeleteMonitoredItems          |           |              |
|                             | ModifyMonitoredItems          |           |              |
|                             | SetMonitoringMode             |           |              |
|                             | SetTriggering                 |           |              |
| Subscription Service Set    | CreateSubscription            |           |              |
|                             | ModifySubscription            |           |              |
|                             | SetPublishingMode             |           |              |
|                             | Publish                       |           |              |
|                             | Republish                     |           |              |
|                             | DeleteSubscriptions           |           |              |
|                             | TransferSubscriptions         |           |              |

---

## PubSub Features

OPCUA PubSub can work with or without an OPCUA client and server.

This component is still in early development on a secondary branch.