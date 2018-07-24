# Socket client for aios server

The aios server is an in java implemeted lightweight server were currently different jdbc datasources can be registered and are accessible throw a socket connection. The exchanged messages an bson en-/decoded.

__To use this package you need an aios server up and running.__

Sources and documentation for the aios server can be found on [https://github.com/thinkbaer/aios](https://github.com/thinkbaer/aios).

__This is an experimantal implementation under active development don't use it on production systems!__



Per default the aios server listens for socket connection on 
 * host: localhost
 * port: 8118

## Usage

### Synchronized (with sockit-to-me)




__Setup the server connection__
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

__Register a JDBC datasource on aios server__
```js
// name for the datasource configuration
var dsn = 'hsql_test'

// necassary specification for datasource
var dsSpec = {
	type: 'jdbc',
	driver: 'org.hsqldb.jdbc.JDBCDriver',
	// if the driverLocation is remote then the driver will be downloaded by the server
	// a local driver path will be directly used 
	driverLocation: "http://central.maven.org/maven2/org/hsqldb/hsqldb/2.3.3/hsqldb-2.3.3.jar",
	url: "jdbc:hsqldb:file:/tmp/test_server/hsql_test",
	user: 'SA',
	password: ''
}

// register the datasource on aios server (needed only once)
var dataSource = server.dataSource(dsn, dsSpec);

// if the datasource is already registered, you can access the dataSource Object through
var sameDataSource = server.dataSource(dsn);
```

__Query the registered datasource__

```js
var createDBSchema =[
    'CREATE TABLE  IF NOT EXISTS car ( id INTEGER IDENTITY, type VARCHAR(256), name VARCHAR(256))'
    "INSERT INTO car (type, name) VALUES('Ford', 'Mustang')",
    "INSERT INTO car (type, name) VALUES('Volkswagen', 'Golf')"
]

// like JDBC executeBatch 
// http://docs.oracle.com/javase/8/docs/api/java/sql/Statement.html#executeBatch--
var results = ds.executeBatch(createDBSchema);

// like JDBC execute
// http://docs.oracle.com/javase/8/docs/api/java/sql/Statement.html#execute--
results = ds.execute("INSERT INTO car (type, name) VALUES('Volvo', 'V70')");

// like JDBC executeUpdate
// http://docs.oracle.com/javase/8/docs/api/java/sql/Statement.html#executeUpdate--
results = ds.update("INSERT INTO car (type, name) VALUES('Volvo', 'V70')");

// like JDBC executeQuery
// http://docs.oracle.com/javase/8/docs/api/java/sql/Statement.html#executeQuery--
results = ds.query("SELECT * FROM car");
// returns an array with query results


```




### Asynchron (with net.Socket)

__Currently not implemented, coming soon ...__

```js
// TODO
```


## Links

__aios server__
 * [github](https://github.com/thinkbaer/aios)
 * [issues](https://github.com/thinkbaer/aios/issues)
 * [wiki](https://github.com/thinkbaer/aios/wiki)

__nodejs aios client__
 * [github](https://github.com/thinkbaer/node-aios)
 * [issues](https://github.com/thinkbaer/node-aios/issues)
 * [wiki](https://github.com/thinkbaer/node-aios/wiki)


----


## Work in progress ...

* Error Handling
* Connection pooling
* Asynchon implementation
* Performance optimization
* Benchmarks

