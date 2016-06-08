# Generate Self Signed Certificates - starting from scratch.

These cfssl steps are suitable only for an AIO! 

cfssl and its utils should have been install by the allineone.sh script into your go path. If it was not you can install it with:

```
go get -u github.com/cloudflare/cfssl/cmd/...
```

See: [https://github.com/cloudflare/cfssl](https://github.com/cloudflare/cfssl) for further info. You'll find a preconfigured ca-config.json, ca cert/key, and sample client configs in /etc/syndicate/cfssl. The instructions that follow will let you configure CFSSL from scratch (and using your hostnames/ip's if you desire).

## Initialize a new Certificate Authority

First generate a ca config and csr using the defaults (cfssl supplies these):

```
cfssl print-defaults config > ca-config.json
cfssl print-defaults csr > ca-csr.json
```

### Configure CA options

We'll only be using a single signing profile of type "client-server" since our servers act as both servers AND clients. This profile will use both the `server auth` and `client auth` extensions (TLS Web Server Auth X509 V3 extension, and TLS Web Client Auth X509 V3 extension). Go ahead and update ca-config.json to look as follows:

```json
{
    "signing": {
        "default": {
            "expiry": "43800h"
        },
        "profiles": {
            "client-server": {
                "expiry": "43800h",
                "usages": [
                    "signing",
                    "key encipherment",
                    "server auth",
                    "client auth"
                ]
            }
        }
    }
}
```


You can also modify `ca-csr.json` to reflect your own details:

```json
{
    "CN": "Oort AIO CA",
    "key": {
        "algo": "rsa",
        "size": 2048
    },
    "names": [
        {
            "C": "US",
            "L": "TX",
            "O": "RAX Cloud Storage",
            "ST": "San Antonio",
            "OU": "Org1",
            "OU": "Org2"
        }
    ]
}
```

Next ask cfssl to generate a CA using the csr above:

```sh
cfssl gencert -initca ca-csr.json | cfssljson -bare ca -
```

That'll create `ca-key.pem`, `ca.pem`, and `ca.csr`. Your key, cert, and request.


### Generate a certificate for use by an oort service

You can now use your ca to generate a key/cert for an oort service:

```
cfssl print-defaults csr > someoorthost.json
```

In the generated config adjust the CN and hosts values to something like:

```
...
    "CN": "someoorthost",
    "hosts": [
        "10.10.10.1",
        "someoorthost.com"
    ],
...
```

Now you can generate the cert and private key using the someoorthost config:

```sh
cfssl gencert -ca=ca.pem -ca-key=ca-key.pem -config=ca-config.json -profile=client-server someoorthost.json | cfssljson -bare someoorthost
```

That will generate your someoorthost-key.pem, someoorthost.pem, and someoorthost.csr files. You can use these key's (along with the ca.pem) on oort hosts.

