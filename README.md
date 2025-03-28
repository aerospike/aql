# AQL

A SQL-like command-line interface to the Aerospike database.

## Cloning the repo
```
git clone --recursive git@github.com:aerospike/aql.git
```

## Installing Dependencies

### Ubuntu 20 and Debian 10
```
sudo apt-get install libreadline8 libreadline-dev flex
```

### Ubuntu 18 and Debian 9
```
sudo apt-get install libreadline7 libreadline-dev flex
```

### Debian 8
```
sudo apt-get install libreadline6 libreadline-dev flex
```

### Red Hat Enterprise Linux and CentOS 7+ or Oracle Linux (Using Curl)
```
yum -y install readline readline-devel flex which
```

### MacOS
```
brew install automake libtool
```

### C client Dependencies
* Install the prerequisites of the C client, as described in its [repo's README](https://github.com/aerospike/aerospike-client-c?tab=readme-ov-file#build-prerequisites).

## Building AQL
```
make clean
make
```

The aql binary will be in

- `target/{target}/bin/aql`

