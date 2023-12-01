# cratedb-wtf

A diagnostics utility in the spirit of [git-wtf], [grafana-wtf], and [pip.wtf].
It is still a work-in-progress, but it is usable already.


## Synopsis

Define CrateDB database cluster address.
```shell
export CRATEDB_SQLALCHEMY_URL=crate://localhost/
```

Display system and database cluster information.
```shell
cratedb-wtf info
```

Display database cluster job information.
```shell
cratedb-wtf job-info
```

Display database cluster log messages.
```shell
cratedb-wtf logs
```

Statistics.
```shell
cratedb-wtf job-statistics collect
cratedb-wtf job-statistics view
```


## HTTP API

Expose collected status information. 
```shell
cratedb-wtf --debug serve --reload
```
Consume collected status information via HTTP.
```shell
http http://127.0.0.1:4242/info/all
```



[git-wtf]: http://thrawn01.org/posts/2014/03/03/git-wtf/ 
[grafana-wtf]: https://github.com/panodata/grafana-wtf
[pip.wtf]: https://github.com/sabslikesobs/pip.wtf
