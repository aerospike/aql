# AQL

A SQL-like command-line interface to the Aerospike database.

### Ubuntu 20 and Debian 10
```
$ sudo apt-get install libreadline8 libreadline-dev flex
```

### Ubuntu 18 and Debian 9
```
$ sudo apt-get install libreadline7 libreadline-dev flex
```

### Debian 8
```
$ sudo apt-get install libreadline6 libreadline-dev flex
```


### Red Hat Enterprise Linux and CentOS 7+ or Oracle Linux (Using Curl)
```
$ yum -y install readline readline-devel flex which
```

### Build the C client

* Clone the C client from GitHub.
    ```
    $ git clone https://github.com/aerospike/aerospike-client-c
    ```

* Install the prerequisites of the C client, as described in its repo's README.
* Build the C client, as described in its repo's README.
* Set the `CLIENTREPO` environment variable to point to the `aerospike-client-c` directory.
```
$ export CLIENTREPO=path/to/aerospike-client-c
```

## Building AQL
	$ make clean
	$ make

The aql binary will be in

- `target/{target}/bin/aql`

