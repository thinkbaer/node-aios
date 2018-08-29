var util = require('util');
var assert = require('assert');
var Connection = require('./../lib/connection');
var Actions = require('./../lib/actions');
var b = require("bson");
var Promise = require("bluebird");



Connection.prototype.connectAsync = Promise.promisify(Connection.prototype.connect);
Connection.prototype.writeAsync = Promise.promisify(Connection.prototype.write);


describe("connection tests", function () {

    it("connect async to server with before declared connect event", function (done) {
        var conn = new Connection();
        conn.on('connect', function (err, _conn) {
            _conn.destroy();
            done(err);
        });
        conn.connect()
    });

    it("connect async to server with direct callback bind", function (done) {
        var conn = new Connection();
        conn.connect(function (err, _conn) {
            _conn.destroy();
            done(err);
        })
    });

    it("connect async with promise bind", function (done) {
        var conn = new Connection();
        conn.connectAsync().then(function (c) {
            c.destroy();
            done();
        }).catch(function (err) {
            console.error(err);
            done(err)
        })
    });


    it("send async ping message over emitter", function (done) {
        var conn = new Connection();

        // Add event listeners
        conn.on('connect', function (err, _conn) {
            var date = new Date();
            var obj = {time: date};
            var t = new Actions.Transport('sys.ping', obj);
            _conn.write(t.toBin());
        });

        conn.on('data', function (err, response) {
            console.log('ping', response.header, response.data);
            conn.destroy();

            assert.equal('sys.ping', response.header.ns);
            assert.equal(true, (response.data.duration > 0));
            done(err)
        });

        conn.connect()
    });


    it("send async ping message over callback", function (done) {
        var conn = new Connection();

        // Add event listeners
        conn.connect(function (err, _conn) {
            var date = new Date();
            var obj = {time: date};

            var t = new Actions.Transport('sys.ping', obj);
            _conn.write(t.toBin(), function (err, response) {
                console.log('ping', response.header, response.data);
                _conn.destroy();

                assert.equal('sys.ping', response.header.ns);
                assert.equal(true, (response.data.duration >= 0));
                done(err)
            });
        })
    });


    it("send async ping message over promise", function (done) {
        var conn = new Connection({debug:true});

        // Add event listeners
        conn.connectAsync().then(function (_conn) {
            var date = new Date();
            var obj = {time: date};

            var t = new Actions.Transport('sys.ping', obj);
            return _conn.writeAsync(t.toBin())
        }).then(function (response) {
            console.log('ping', response.header, response.data);

            assert.equal('sys.ping', response.header.ns);
            assert.equal(true, (response.data.duration >= 0));
            return conn.destroy();
        }).then(function () {
            assert.equal(false, conn.isConnected());
            done()
        }).catch(function (err) {
            done(err)
        });
    });


    it("asynchronized create db and query it", function (done) {
        this.timeout(10000);

        var dsn = 'hsql_test';
        var ds = {
            method: 'register',
            spec: {
                type: 'jdbc',
                name: dsn,
                driver: 'org.hsqldb.jdbc.JDBCDriver',
                driverLocation: "http://central.maven.org/maven2/org/hsqldb/hsqldb/2.3.3/hsqldb-2.3.3.jar",
                url: "jdbc:hsqldb:file:/tmp/testdb/hsql1",
                user: 'SA',
                password: ''
            }
        };

        var dsInitDB = {
            dsn: dsn,
            query: {
                '@t': 'jdbc.q.batch',
                sqls: [
                    'CREATE TABLE  IF NOT EXISTS car ( id INTEGER IDENTITY, type VARCHAR(256), name VARCHAR(256))',
                    "CREATE TABLE  IF NOT EXISTS owner ( id INTEGER IDENTITY, surname VARCHAR(256), givenName VARCHAR(256))",
                    "INSERT INTO car (type, name) VALUES('Ford', 'Mustang')",
                    "INSERT INTO car (type, name) VALUES('Ford', 'Fiesta')",
                    "INSERT INTO owner (surname,givenName) VALUES('Ford', 'Henry')"
                ]
            }
        };

        var dsQueryDB = {
            dsn: dsn,
            query: {
                '@t': 'jdbc.q.select',
                sql: 'SELECT * FROM car'
            }
        };


        var dsQueryDB2 = {
            dsn: dsn,
            query: {
                '@t': 'jdbc.q.update',
                sql: 'TRUNCATE TABLE car'
            }
        };

        var dsQueryDB3 = {
            dsn: dsn,
            query: {
                '@t': 'jdbc.q.exec',
                sql: 'TRUNCATE TABLE car'
            }
        };

        var conn = new Connection();
        conn.connectAsync()
            .then(function (_conn) {
                var t = new Actions.Transport('ds', ds);
                return _conn.writeAsync(t.toBin());
            })
            .then(function (response) {
                console.log('request', ds);
                console.log('header', response.header);
                console.log('data', response.data);

                var t = new Actions.Transport('ds.query', dsInitDB);
                return conn.writeAsync(t.toBin());
            })
            .then(function (response) {
                console.log('request', dsInitDB);
                console.log('header', response.header);
                console.log('data', response.data);

                var t = new Actions.Transport('ds.query', dsQueryDB);
                return conn.writeAsync(t.toBin());
            })
            .then(function (response) {
                console.log('request', dsQueryDB);
                console.log('header', response.header);
                console.log('data', response.data);
                console.log('result', response.data.result.data);

                var t = new Actions.Transport('ds.query', dsQueryDB2);
                return conn.writeAsync(t.toBin());
            })
            .then(function (response) {
                console.log('request', dsQueryDB2);
                console.log('header', response.header);
                console.log('data', response.data);
                console.log('result', response.data.result.data);

                var t = new Actions.Transport('ds.query', dsQueryDB3);
                return conn.writeAsync(t.toBin());
            })
            .then(function (response) {
                console.log('request', dsQueryDB2);
                console.log('header', response.header);
                console.log('data', response.data);
                console.log('result', response.data.result.data);

                return conn.destroy();
            })
            .then(function(){
                done()
            })
            .catch(function(err){
                done(err)
            })

    });


    it("asynchronized list tables", function (done) {
        this.timeout(10000);

        var dsn = 'hsql_test';
        var ds = {
            method: 'register',
            spec: {
                type: 'jdbc',
                name: dsn,
                driver: 'org.hsqldb.jdbc.JDBCDriver',
                driverLocation: "http://central.maven.org/maven2/org/hsqldb/hsqldb/2.3.3/hsqldb-2.3.3.jar",
                url: "jdbc:hsqldb:file:/tmp/testdb/hsql1",
                user: 'SA',
                password: ''
            }
        };

        var dsInitDB = {
            dsn: dsn,
            query: {
                '@t': 'jdbc.q.batch',
                sqls: [
                    'CREATE TABLE  IF NOT EXISTS car ( id INTEGER IDENTITY, type VARCHAR(256), name VARCHAR(256))',
                    "CREATE TABLE  IF NOT EXISTS owner ( id INTEGER IDENTITY, surname VARCHAR(256), givenName VARCHAR(256))",
                    "INSERT INTO car (type, name) VALUES('Ford', 'Mustang')",
                    "INSERT INTO car (type, name) VALUES('Ford', 'Fiesta')",
                    "INSERT INTO owner (surname,givenName) VALUES('Ford', 'Henry')"
                ]
            }
        };

        var dsQueryDB = {
            dsn: dsn,
            query: {
                '@t': 'jdbc.q.table',
                catalog:'PUBLIC',
            }
        };



        var conn = new Connection();
        conn.connectAsync()
            .then(function (_conn) {
                var t = new Actions.Transport('ds', ds);
                return _conn.writeAsync(t.toBin());
            })
            .then(function (response) {
                var t = new Actions.Transport('ds.query', dsInitDB);
                return conn.writeAsync(t.toBin());
            })
            .then(function (response) {
                var t = new Actions.Transport('ds.query', dsQueryDB);
                return conn.writeAsync(t.toBin());
            })
            .then(function (response) {
                console.log(util.inspect(response));

                return conn.destroy();
            })
            .then(function(){
                done()
            })
            .catch(function(err){
                done(err)
            })

    })

});

