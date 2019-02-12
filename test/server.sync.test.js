var assert = require('assert');
var Server = require('./../lib/server');

var dsn = 'hsql_test_02';
var dsSpec = {
  type: 'jdbc',
  driver: 'org.hsqldb.jdbc.JDBCDriver',
  driverLocation: "http://central.maven.org/maven2/org/hsqldb/hsqldb/2.3.3/hsqldb-2.3.3.jar",
  url: "jdbc:hsqldb:file:/tmp/test_server/hsql1",
  user: 'SA',
  password: ''
};

var createDBSchema = [
  'CREATE TABLE  IF NOT EXISTS car ( id INTEGER IDENTITY, type VARCHAR(256), name VARCHAR(256))',
  "CREATE TABLE  IF NOT EXISTS owner ( id INTEGER IDENTITY, surname VARCHAR(256), givenName VARCHAR(256))",
];

var insertData = [
  "INSERT INTO car (type, name) VALUES('Ford', 'Mustang')",
  "INSERT INTO car (type, name) VALUES('Ford', 'Fiesta')",
  "INSERT INTO owner (surname,givenName) VALUES('Ford', 'Henry')"

];

var server = null;
describe("server tests", function () {

  describe("synchronized requests", function () {

    server = new Server();

    it("ping", function () {
      var result = server.ping();
      assert.equal(true, result.duration > 0);
    });

    it("register datasource", function () {
      var ds = server.dataSource(dsn, dsSpec);
      assert.equal(true, ds.getId() > 0);
    });

    it("get registered datasource", function () {
      var ds = server.dataSource(dsn);
      assert.equal(true, ds.getId() > 0);
    });

    it("create db", function () {
      var ds = server.dataSource(dsn);
      assert.equal(true, ds.getId() > 0);

      var results = ds.executeBatch(createDBSchema);
      assert.equal(2, results.batchResults.length);
    });

    it("clear tables db", function () {
      var ds = server.dataSource(dsn);
      assert.equal(true, ds.getId() > 0);

      var results = ds.execute('TRUNCATE TABLE car');
      assert.equal(1, results.affected);

      var results = ds.execute('TRUNCATE TABLE owner');
      assert.equal(1, results.affected);

    });

    it("insert data", function () {
      var ds = server.dataSource(dsn);
      assert.equal(true, ds.getId() > 0);

      var results = ds.executeBatch(insertData);
      assert.equal(3, results.batchResults.length);
    });

    it("update data", function () {
      var ds = server.dataSource(dsn);
      assert.equal(true, ds.getId() > 0);

      var results = ds.update("INSERT INTO car (type, name) VALUES('Volvo', 'V70')");
      assert.equal(1, results.affected);
      assert.equal(1, results.data.length);
    });

    it("query data", function () {
      var ds = server.dataSource(dsn);
      assert.equal(true, ds.getId() > 0);

      var results = ds.query("SELECT * FROM car");
      assert.equal(3, results.length);
    })

  })

});
