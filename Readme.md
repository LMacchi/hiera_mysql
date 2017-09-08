# Mysql backend for Hiera 5

Trying to understand hiera 5 with the help of my friend MySQL.

Inspiration: [crayfishx/hiera-http](https://github.com/crayfishx/hiera-http)

NOT PROD READY! It is a very much work in progress

Hiera.yaml:

```
  - name: "MySQL"
    lookup_key: mysql_lookup_key
    options:
      host: mysql.puppetlabs.com
      user: hiera
      pass: hiera123
      database: hiera
      # query = select %{value_field} from %{table} where %{key_field}="%{key}"
      table: configdata
      value_field: val
      key_field: var 
```

Data in configdata table:

```
MariaDB [hiera]> select val from configdata where var = 'message';
+-------+
| val   |
+-------+
| hello |
+-------+
1 row in set (0.01 sec)
```

Result:

```
root@master /root> puppet lookup --node $(facter fqdn) --explain message
Searching for "message"
  Global Data Provider (hiera configuration version 5)
    Using configuration "/etc/puppetlabs/puppet/hiera.yaml"
    Hierarchy entry "MySQL"
      Found key: "message" value: "hello"
```
