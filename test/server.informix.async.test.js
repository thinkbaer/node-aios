var assert = require('assert');
var Server = require('./../lib/server');

describe("server informix async tests", function () {

  var dsn_informix = 'informix_test_01';
  var dsSpec_informix = {
    type: 'jdbc',
    driver: 'com.informix.jdbc.IfxDriver',
    driverLocation: "/data/java/driver/com.informix.ifxjdbc-4.10.JC4DE.jar",
    url: "jdbc:informix-sqli://informix:9088/iot:INFORMIXSERVER=informix;DELIMITER=",
    user: 'informix',
    password: 'in4mix',
    debug: true
  };

  var createDBSchema_informix = [
    'CREATE TABLE  IF NOT EXISTS car ( id SERIAL, type VARCHAR(32), name VARCHAR(32))',
    "CREATE TABLE  IF NOT EXISTS owner ( id SERIAL, surname VARCHAR(32), givenName VARCHAR(32))",
  ];

  var insertData_informix = [
    "INSERT INTO car (type, name) VALUES('Ford', 'Mustang')",
    "INSERT INTO car (type, name) VALUES('Ford', 'Fiesta')",
    "INSERT INTO owner (surname,givenName) VALUES('Ford', 'Henry')"
  ];


  var server_ifx = null;

  describe("asynchronized requests", function () {
    this.timeout(20000);

    server_ifx = new Server();

    it("ping", function (done) {
      server_ifx.ping(function (err, res) {
        assert.equal(true, res.duration > 0);
        done(err)
      })

    });


    it("register datasource", function (done) {
      server_ifx.dataSource(dsn_informix, dsSpec_informix, function (err, res) {
        assert.equal(true, res.getId() > 0);
        done(err)
      });
    });


    it("get registered datasource", function (done) {
      server_ifx.dataSource(dsn_informix, function (err, res) {
        assert.equal(true, res.getId() > 0);
        done(err)
      });

    });

    it("create db tables", function (done) {
      var ds = server_ifx.dataSource(dsn_informix);
      assert.equal(true, ds.getId() > 0);

      ds.executeBatch(createDBSchema_informix, function (err, res) {
        assert.equal(null,err);
        assert.equal(2, res.batchResults.length);
        done(err)
      });

    });


    it("clear tables db", function (done) {
      var ds = server_ifx.dataSource(dsn_informix);
      assert.equal(true, ds.getId() > 0);

      ds.execute('TRUNCATE TABLE car', function (err, res) {
        assert.equal(0, res.affected);

        ds.execute('TRUNCATE TABLE owner', function (err2, res2) {
          assert.equal(0, res.affected);
          done(err2)
        });
      });


    });

    it("insert data", function (done) {
      var ds = server_ifx.dataSource(dsn_informix);
      assert.equal(true, ds.getId() > 0);

      ds.executeBatch(insertData_informix, function (err, res) {
        assert.equal(3, res.batchResults.length);
        done(err);
      });

    });


    it("update data", function (done) {
      var ds = server_ifx.dataSource(dsn_informix);
      assert.equal(true, ds.getId() > 0);

      ds.update("INSERT INTO car (type, name) VALUES('Volvo', 'V70')", function (err, res) {
        assert.equal(1, res.affected);
        assert.equal(1, res.data.length);
        done(err);
      });

    });

    it("query data", function (done) {
      var ds = server_ifx.dataSource(dsn_informix);
      assert.equal(true, ds.getId() > 0);

      ds.query("SELECT * FROM car", function (err, res) {
        assert.equal(3, res.length);
        done(err);
      });

    });

    it("list catalogs", function (done) {
      var ds = server_ifx.dataSource(dsn_informix);
      assert.equal(true, ds.getId() > 0);

      ds.listCatalogs(function (err, res) {
        assert.equal(true,res.length > 0);
        done(err);
      });

    });

    it("list tables", function (done) {
      var ds = server_ifx.dataSource(dsn_informix);
      assert.equal(true, ds.getId() > 0);

      /*
      ds.listTables('PUBLIC');
      done();
      */

      ds.listTables('iot',function (err, res) {
        assert.equal(true,res.length > 0);
        done(err);
      });


    })

  })

});
