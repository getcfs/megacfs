## You need to ether be running oort with the redis protocol, or run a redis server instance.

<pre>
$ ./formic -h
Usage of ./formic:
  -cert_file="server.crt": The TLS cert file
  -key_file="server.key": The TLS key file
  -oorthost="127.0.0.1:6379": host:port to use when connecting to oort
  -port=8443: The server port
  -tls=true: Connection uses TLS if true, else plain TCP
</pre>

*To run*

<pre>
go build . && ./formic
</pre>

## formicd command line options(with defaults):

Option | default | description
------ | ------- | -----------
usetls | true | Connection uses TLS if true, else plain TCP
certFile | /etc/oort/server.crt | The TLS cert file
keyFile | /etc/oort/server.key | The TLS key file
port | 9443 | The server port
oortValueHost | 127.0.0.1:6379 | host:port to use when connecting to oort value
oortGroupHost | 127.0.0.1:6380 | host:port to use when connecting to oort group
insecureSkipVerify | false | don't verify cert
oortClientMutualTLS | false | whether or not the server expects mutual tls auth
oortClientCert | /etc/oort/client.crt | cert file to use
oortClientKey | /etc/oort/client.key | key file to use
oortClientCA | /etc/oort/ca.pem | ca file to use



## To run as a deamon with systemd:

<pre>
cp -av packaging/root/usr/share/formicd/systemd/formicd.service /lib/systemd/system
</pre>

To override any defaults add the new config options into the /etc/default/formicd file:
* FORMICD_TLS
* FORMICD_OORT_VALUE_HOST
* FORMICD_OORT_GROUP_HOST
* FORMICD_PORT
* FORMICD_CERT_FILE
* FORMICD_KEY_FILE
* FORMICD_INSECURE_SKIP_VERIFY
* FORMICD_MUTUAL_TLS
* FORMICD_CLIENT_CA_FILE
* FORMICD_CLIENT_CERT_FILE
* FORMICD_CLIENT_KEY_FILE

*Example:*

<pre>
echo 'FORMICD_PORT=8444' > /etc/default/formicd
</pre>
