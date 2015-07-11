# Socket client for Aios server

The Aios server is an in java implemted lightweight server, were different 

Documentation of 

https://github.com/thinkbaer/aios


## Implementation

Both the 

## Usage

### Synchronized (with sockit-to-me)


Setup the 
```js
var AiosServer = require('aios');

/**
 * Default implementation
 */
var server = new AiosServer({host:'localhost',port:8118});

// Sends the current timestamp and receives server time and duration
var response = server.ping();
/*
resonpse = {
	time: DateObject
	duration: long
}
*/
```

Register a JDBC datasource on aios server
```js
// name for the datasource configuration
var dsn = 'hsql_test'
var dsSpec = {
        type: 'jdbc',
        driver: 'org.hsqldb.jdbc.JDBCDriver',
        driverLocation: "http://central.maven.org/maven2/org/hsqldb/hsqldb/2.3.3/hsqldb-2.3.3.jar",
        url: "jdbc:hsqldb:file:/tmp/test_server/hsql1",
        user: 'SA',
        password: ''
}

var dataSource = server.dataSource(dsn, dsSpec);


```


```js
var createDBSchema =[
    'CREATE TABLE  IF NOT EXISTS car ( id INTEGER IDENTITY, type VARCHAR(256), name VARCHAR(256))'
]

var insertData = [
    "INSERT INTO car (type, name) VALUES('Ford', 'Mustang')",
    "INSERT INTO car (type, name) VALUES('Ford', 'Fiesta')"
]

```




### Asynchron (with net.Socket)

__Currently not implemented, coming soon ...__

```js
// TODO
```


----


## Todo

* Error Handling
* Connection pooling
* 

