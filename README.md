# AQL

A SQL-like command-line interface to the Aerospike database.

## Build Prerequisites

- curl
- zip
- OpenSSL >= 1.1.1k

### Ubuntu 18+ and Debian 8+

```
$ sudo apt-get install libreadline6 libreadline-dev flex

Note for debian:9 and ubuntu 18 use libreadline7

For Packaging

$ apt-get install zip rsync fakeroot 
```

### Red Hat Enterprise Linux and CentOS 7+ or Oracle Linux (Using Curl)
```
$ yum -y install readline readline-devel flex which

For Packaging

$ yum -y install zip rpmdevtools
```

### Build the C client

* Clone the C client from GitHub.
    ```
    $ git clone https://github.com/aerospike/aerospike-client-c
    ```

* Install the prerequisites of the C client, as described in its repo's README.
* Build the C client, as described in its repo's README.
* Set the `CLIENTREPO` environment variable to point to the `aerospike-client-c` directory.

### Build the latest 2.x Jansson

	$ cd jansson/
	$ autoreconf -i
	$ ./configure
	$ make

### Build the toml

	$ cd toml/
	$ make

## Building AQL

	$ make clean
	$ make

The aql binary will be in

- `target/{target}/bin/aql`

