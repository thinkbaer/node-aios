import * as assert from 'assert';
import {Server} from './../../src/server';


describe('server async tests', function () {

  const dsn_default: any = 'hsql_test_01';
  const dsSpec_default: any = {
    type: 'jdbc',
    driver: 'org.hsqldb.jdbc.JDBCDriver',
    driverLocation: 'https://repo1.maven.org/maven2/org/hsqldb/hsqldb/2.5.0/hsqldb-2.5.0.jar',
    url: 'jdbc:hsqldb:file:/tmp/test_server/hsql1',
    user: 'SA',
    password: ''
  };

  const createDBSchema_default: any = [
    'CREATE TABLE  IF NOT EXISTS car ( id INTEGER IDENTITY, type VARCHAR(256), name VARCHAR(256))',
    'CREATE TABLE  IF NOT EXISTS owner ( id INTEGER IDENTITY, surname VARCHAR(256), givenName VARCHAR(256))',
  ];

  const insertData_default = [
    'INSERT INTO car (type, name) VALUES(\'Ford\', \'Mustang\')',
    'INSERT INTO car (type, name) VALUES(\'Ford\', \'Fiesta\')',
    'INSERT INTO owner (surname,givenName) VALUES(\'Ford\', \'Henry\')'

  ];


  let server: any = null;
  describe('asynchronized requests', function () {

    // @ts-ignore
    server = new Server();

    it('ping', function (done) {
      server.ping(function (err: any, res: any) {
        assert.equal(true, res.duration > 0);
        done(err);
      });

    });


    it('register datasource', function (done) {
      server.dataSource(dsn_default, dsSpec_default, function (err: any, res: any) {
        assert.equal(true, res.getId() > 0);
        done(err);
      });
    });


    it('get registered datasource', function (done) {
      server.dataSource(dsn_default, function (err: any, res: any) {
        assert.equal(true, res.getId() > 0);
        done(err);
      });

    });

    it('create db', function (done) {
      const ds = server.dataSource(dsn_default);
      assert.equal(true, ds.getId() > 0);

      ds.executeBatch(createDBSchema_default, function (err: any, res: any) {
        assert.equal(null, err);
        assert.equal(2, res.batchResults.length);
        done(err);
      });

    });


    it('clear tables db', function (done) {
      const ds = server.dataSource(dsn_default);
      assert.equal(true, ds.getId() > 0);

      ds.execute('TRUNCATE TABLE car', function (err: any, res: any) {
        assert.equal(1, res.affected);


        ds.execute('TRUNCATE TABLE owner', function (err2: any, res2: any) {
          assert.equal(1, res.affected);
          done(err2);
        });
      });


    });

    it('insert data', function (done) {
      const ds = server.dataSource(dsn_default);
      assert.equal(true, ds.getId() > 0);

      ds.executeBatch(insertData_default, function (err: any, res: any) {
        assert.equal(3, res.batchResults.length);
        done(err);
      });

    });


    it('update data', function (done) {
      const ds = server.dataSource(dsn_default);
      assert.equal(true, ds.getId() > 0);

      ds.update('INSERT INTO car (type, name) VALUES(\'Volvo\', \'V70\')', function (err: any, res: any) {
        assert.equal(1, res.affected);
        assert.equal(1, res.data.length);
        done(err);
      });

    });

    it('query data', function (done) {
      const ds = server.dataSource(dsn_default);
      assert.equal(true, ds.getId() > 0);

      ds.query('SELECT * FROM car', function (err: any, res: any) {
        assert.equal(3, res.length);
        done(err);
      });

    });

    it('list catalogs', function (done) {
      const ds = server.dataSource(dsn_default);
      assert.equal(true, ds.getId() > 0);

      ds.listCatalogs(function (err: any, res: any) {
        done(err);
      });

    });

    it('list tables', function (done) {
      const ds = server.dataSource(dsn_default);
      assert.equal(true, ds.getId() > 0);

      /*
      ds.listTables('PUBLIC');
      done();
      */

      ds.listTables('PUBLIC', function (err: any, res: any) {
        done(err);
      });


    });

  });

});
