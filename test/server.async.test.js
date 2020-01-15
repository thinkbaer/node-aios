var assert = require('assert');
var Server = require('./../lib/server');


describe("server async tests", function () {

  var dsn_default = 'hsql_test_01';
  var dsSpec_default = {
    type: 'jdbc',
    driver: 'org.hsqldb.jdbc.JDBCDriver',
    driverLocation: "https://repo1.maven.org/maven2/org/hsqldb/hsqldb/2.5.0/hsqldb-2.5.0.jar",
    url: "jdbc:hsqldb:file:/tmp/test_server/hsql1",
    user: 'SA',
    password: ''
  };

  var createDBSchema_default = [
    'CREATE TABLE  IF NOT EXISTS car ( id INTEGER IDENTITY, type VARCHAR(256), name VARCHAR(256))',
    "CREATE TABLE  IF NOT EXISTS owner ( id INTEGER IDENTITY, surname VARCHAR(256), givenName VARCHAR(256))",
  ];

  var insertData_default = [
    "INSERT INTO car (type, name) VALUES('Ford', 'Mustang')",
    "INSERT INTO car (type, name) VALUES('Ford', 'Fiesta')",
    "INSERT INTO owner (surname,givenName) VALUES('Ford', 'Henry')"

  ];


  var server = null;
  describe("asynchronized requests", function () {

    server = new Server();

    it("ping", function (done) {
      server.ping(function (err, res) {
        assert.equal(true, res.duration > 0);
        done(err)
      })

    });


    it("register datasource", function (done) {
      server.dataSource(dsn_default, dsSpec_default, function (err, res) {
        assert.equal(true, res.getId() > 0);
        done(err)
      });
    });


    it("get registered datasource", function (done) {
      server.dataSource(dsn_default, function (err, res) {
        assert.equal(true, res.getId() > 0);
        done(err)
      });

    });

    it("create db", function (done) {
      var ds = server.dataSource(dsn_default);
      assert.equal(true, ds.getId() > 0);

      ds.executeBatch(createDBSchema_default, function (err, res) {
        assert.equal(null,err);
        assert.equal(2, res.batchResults.length);
        done(err)
      });

    });


    it("clear tables db", function (done) {
      var ds = server.dataSource(dsn_default);
      assert.equal(true, ds.getId() > 0);

      ds.execute('TRUNCATE TABLE car', function (err, res) {
        assert.equal(1, res.affected);


        ds.execute('TRUNCATE TABLE owner', function (err2, res2) {
          assert.equal(1, res.affected);
          done(err2)
        });
      });


    });

    it("insert data", function (done) {
      var ds = server.dataSource(dsn_default);
      assert.equal(true, ds.getId() > 0);

      ds.executeBatch(insertData_default, function (err, res) {
        assert.equal(3, res.batchResults.length);
        done(err);
      });

    });


    it("update data", function (done) {
      var ds = server.dataSource(dsn_default);
      assert.equal(true, ds.getId() > 0);

      ds.update("INSERT INTO car (type, name) VALUES('Volvo', 'V70')", function (err, res) {
        assert.equal(1, res.affected);
        assert.equal(1, res.data.length);
        done(err);
      });

    });

    it("query data", function (done) {
      var ds = server.dataSource(dsn_default);
      assert.equal(true, ds.getId() > 0);

      ds.query("SELECT * FROM car", function (err, res) {
        assert.equal(3, res.length);
        done(err);
      });

    });

    it("list catalogs", function (done) {
      var ds = server.dataSource(dsn_default);
      assert.equal(true, ds.getId() > 0);

      ds.listCatalogs(function (err, res) {
        console.log(res);
        done(err);
      });

    });

    it("list tables", function (done) {
      var ds = server.dataSource(dsn_default);
      assert.equal(true, ds.getId() > 0);

      /*
      ds.listTables('PUBLIC');
      done();
      */

      ds.listTables('PUBLIC', function (err, res) {
        console.log(res);
        done(err);
      });


    })

  })

});
