var assert = require('assert');
var Connection = require('./../lib/connection');
var Actions = require('./../lib/actions');

describe("connection tests", function () {



    it("connect sync to server", function () {
        var conn = new Connection();
        conn.connect();
        conn.destroy();

    });



    it("send sync ping message", function () {
        var conn = new Connection();
        conn.connect();

        var date = new Date();
        var obj = {time: date};
        var t = new Actions.Transport('sys.ping', obj);
        var response = conn.write(t.toBin());

        conn.destroy();

        assert.equal('sys.ping', response.header.ns);
        assert.equal(true, (response.data.duration > 0));
    });

    it("synchronized create db and query it", function () {
        this.timeout(10000);
        var conn = new Connection();
        conn.connect();

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
            query:{
                '@t':'jdbc.q.batch',
                sqls:[
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
            query:{
                '@t':'jdbc.q.select',
                sql:'SELECT * FROM car'
            }
        };


        var dsQueryDB2 = {
            dsn: dsn,
            query:{
                '@t':'jdbc.q.update',
                sql:'TRUNCATE TABLE car'
            }
        };

        var dsQueryDB3 = {
            dsn: dsn,
            query:{
                '@t':'jdbc.q.exec',
                sql:'TRUNCATE TABLE car'
            }
        };

        var t = new Actions.Transport('ds', ds);
        var response = conn.write(t.toBin());

        console.log('request', ds);
        console.log('header', response.header);
        console.log('data', response.data);

        t = new Actions.Transport('ds.query', dsInitDB);
        response = conn.write(t.toBin());

        console.log('request', dsInitDB);
        console.log('header', response.header);
        console.log('data', response.data);

        t = new Actions.Transport('ds.query', dsQueryDB);
        response = conn.write(t.toBin());

        console.log('request', dsQueryDB);
        console.log('header', response.header);
        console.log('data', response.data);
        console.log('result', response.data.result.data);

        t = new Actions.Transport('ds.query', dsQueryDB2);
        response = conn.write(t.toBin());

        console.log('request', dsQueryDB);
        console.log('header', response.header);
        console.log('data', response.data);


        t = new Actions.Transport('ds.query', dsQueryDB3);
        response = conn.write(t.toBin());

        console.log('request', dsQueryDB);
        console.log('header', response.header);
        console.log('data', response.data);


        conn.destroy()

    })

});