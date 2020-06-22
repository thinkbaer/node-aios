import * as util from 'util';
import * as assert from 'assert';
import * as Actions from './../../src/actions';
import {Connection} from './../../src/connection';


// @ts-ignore
describe('connection async tests', function () {

  it('connect async to server with before declared connect event', function (done) {
    // @ts-ignore
    let conn = new Connection();
    conn.on('connect', function (err: any, _conn: any) {
      _conn.destroy();
      done(err);
    });
    conn.connect();
  });

  it('connect async to server with direct callback bind', function (done) {
    // @ts-ignore
    let conn = new Connection();
    conn.connect(function (err: any, _conn: any) {
      _conn.destroy();
      done(err);
    });
  });

  it('connect async with promise bind', function (done) {
    // @ts-ignore
    let conn = new Connection();
    conn.connectAsync().then(function (c: any) {
      c.destroy();
      done();
    }).catch(function (err: any) {
      console.error(err);
      done(err);
    });
  });


  it('send async ping message over emitter', function (done) {
    // @ts-ignore
    let conn = new Connection();

    // Add event listeners
    conn.on('connect', function (err: any, _conn: any) {
      let date = new Date();
      let obj = {time: date};
      // @ts-ignore
      let t = new Actions.Transport('sys.ping', obj);
      _conn.write(t.toBin());
    });

    conn.on('data', function (err: any, response: any) {
      conn.destroy();

      assert.equal('sys.ping', response.header.ns);
      assert.equal(true, (response.data.duration > 0));
      done(err);
    });

    conn.connect();
  });


  it('send async ping message over callback', function (done) {
    // @ts-ignore
    let conn = new Connection();

    // Add event listeners
    conn.connect(function (err: any, _conn: any) {
      let date = new Date();
      let obj = {time: date};

      // @ts-ignore
      let t = new Actions.Transport('sys.ping', obj);
      _conn.write(t.toBin(), function (err: any, response: any) {
        // console.log('ping', response.header, response.data);
        _conn.destroy();

        assert.equal('sys.ping', response.header.ns);
        assert.equal(true, (response.data.duration >= 0));
        done(err);
      });
    });
  });


  it('send async ping message over promise', function (done) {
    // @ts-ignore
    let conn = new Connection({debug: true});

    // Add event listeners
    conn.connectAsync().then(function (_conn: any) {
      let date = new Date();
      let obj = {time: date};

      // @ts-ignore
      let t = new Actions.Transport('sys.ping', obj);
      return _conn.writeAsync(t.toBin());
    }).then(function (response: any) {
      // console.log('ping', response.header, response.data);

      assert.equal('sys.ping', response.header.ns);
      assert.equal(true, (response.data.duration >= 0));
      return conn.destroy();
    }).then(function () {
      assert.equal(false, conn.isConnected());
      done();
    }).catch(function (err: any) {
      done(err);
    });
  });


  it('asynchronized create db and query it', function (done) {
    this.timeout(10000);

    let dsn = 'hsql_test_03';
    let ds = {
      method: 'register',
      spec: {
        type: 'jdbc',
        name: dsn,
        driver: 'org.hsqldb.jdbc.JDBCDriver',
        driverLocation: 'https://repo1.maven.org/maven2/org/hsqldb/hsqldb/2.5.0/hsqldb-2.5.0.jar',
        url: 'jdbc:hsqldb:file:/tmp/testdb/hsql1',
        user: 'SA',
        password: ''
      }
    };

    let dsInitDB = {
      dsn: dsn,
      query: {
        '@t': 'jdbc.q.batch',
        sqls: [
          'CREATE TABLE  IF NOT EXISTS car ( id INTEGER IDENTITY, type VARCHAR(256), name VARCHAR(256))',
          'CREATE TABLE  IF NOT EXISTS owner ( id INTEGER IDENTITY, surname VARCHAR(256), givenName VARCHAR(256))',
          'INSERT INTO car (type, name) VALUES(\'Ford\', \'Mustang\')',
          'INSERT INTO car (type, name) VALUES(\'Ford\', \'Fiesta\')',
          'INSERT INTO owner (surname,givenName) VALUES(\'Ford\', \'Henry\')'
        ]
      }
    };

    let dsQueryDB = {
      dsn: dsn,
      query: {
        '@t': 'jdbc.q.select',
        sql: 'SELECT * FROM car'
      }
    };


    let dsQueryDB2 = {
      dsn: dsn,
      query: {
        '@t': 'jdbc.q.update',
        sql: 'TRUNCATE TABLE car'
      }
    };

    let dsQueryDB3 = {
      dsn: dsn,
      query: {
        '@t': 'jdbc.q.exec',
        sql: 'TRUNCATE TABLE car'
      }
    };

    // @ts-ignore
    let conn = new Connection();
    conn.connectAsync()
      .then(function (_conn: any) {
        // @ts-ignore
        let t = new Actions.Transport('ds', ds);
        return _conn.writeAsync(t.toBin());
      })
      .then(function (response: any) {
        // console.log('request', ds);
        // console.log('header', response.header);
        // console.log('data', response.data);

        // @ts-ignore
        let t = new Actions.Transport('ds.query', dsInitDB);
        return conn.writeAsync(t.toBin());
      })
      .then(function (response: any) {
        // console.log('request', dsInitDB);
        // console.log('header', response.header);
        // console.log('data', response.data);

        // @ts-ignore
        let t = new Actions.Transport('ds.query', dsQueryDB);
        return conn.writeAsync(t.toBin());
      })
      .then(function (response: any) {
        // console.log('request', dsQueryDB);
        // console.log('header', response.header);
        // console.log('data', response.data);
        // console.log('result', response.data.result.data);

        // @ts-ignore
        let t = new Actions.Transport('ds.query', dsQueryDB2);
        return conn.writeAsync(t.toBin());
      })
      .then(function (response: any) {
        // console.log('request', dsQueryDB2);
        // console.log('header', response.header);
        // console.log('data', response.data);
        // console.log('result', response.data.result.data);

        // @ts-ignore
        let t = new Actions.Transport('ds.query', dsQueryDB3);
        return conn.writeAsync(t.toBin());
      })
      .then(function (response: any) {
        // console.log('request', dsQueryDB2);
        // console.log('header', response.header);
        // console.log('data', response.data);
        // console.log('result', response.data.result.data);

        return conn.destroy();
      })
      .then(function () {
        done();
      })
      .catch(function (err: any) {
        done(err);
      });

  });


  it('asynchronized list tables', function (done) {
    this.timeout(10000);

    let dsn = 'hsql_test_03';
    let ds = {
      method: 'register',
      spec: {
        type: 'jdbc',
        name: dsn,
        driver: 'org.hsqldb.jdbc.JDBCDriver',
        driverLocation: 'https://repo1.maven.org/maven2/org/hsqldb/hsqldb/2.5.0/hsqldb-2.5.0.jar',
        url: 'jdbc:hsqldb:file:/tmp/testdb/hsql1',
        user: 'SA',
        password: ''
      }
    };

    let dsInitDB = {
      dsn: dsn,
      query: {
        '@t': 'jdbc.q.batch',
        sqls: [
          'CREATE TABLE  IF NOT EXISTS car ( id INTEGER IDENTITY, type VARCHAR(256), name VARCHAR(256))',
          'CREATE TABLE  IF NOT EXISTS owner ( id INTEGER IDENTITY, surname VARCHAR(256), givenName VARCHAR(256))',
          'INSERT INTO car (type, name) VALUES(\'Ford\', \'Mustang\')',
          'INSERT INTO car (type, name) VALUES(\'Ford\', \'Fiesta\')',
          'INSERT INTO owner (surname,givenName) VALUES(\'Ford\', \'Henry\')'
        ]
      }
    };

    let dsQueryDB = {
      dsn: dsn,
      query: {
        '@t': 'jdbc.q.table',
        catalog: 'PUBLIC',
      }
    };


    // @ts-ignore
    let conn = new Connection();
    conn.connectAsync()
      .then(function (_conn: any) {
        // @ts-ignore
        let t = new Actions.Transport('ds', ds);
        return _conn.writeAsync(t.toBin());
      })
      .then(function (response: any) {
        // @ts-ignore
        let t = new Actions.Transport('ds.query', dsInitDB);
        return conn.writeAsync(t.toBin());
      })
      .then(function (response: any) {
        // @ts-ignore
        let t = new Actions.Transport('ds.query', dsQueryDB);
        return conn.writeAsync(t.toBin());
      })
      .then(function (response: any) {
        // console.log(util.inspect(response));

        return conn.destroy();
      })
      .then(function () {
        done();
      })
      .catch(function (err: any) {
        done(err);
      });

  });

});

