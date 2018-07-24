var assert = require('assert');
var Server = require('./../lib/server')

var dsn = 'hsql_test'
var dsSpec = {
        type: 'jdbc',
        driver: 'org.hsqldb.jdbc.JDBCDriver',
        driverLocation: "http://central.maven.org/maven2/org/hsqldb/hsqldb/2.3.3/hsqldb-2.3.3.jar",
        url: "jdbc:hsqldb:file:/tmp/test_server/hsql1",
        user: 'SA',
        password: ''
}

var createDBSchema =[
    'CREATE TABLE  IF NOT EXISTS car ( id INTEGER IDENTITY, type VARCHAR(256), name VARCHAR(256))',
    "CREATE TABLE  IF NOT EXISTS owner ( id INTEGER IDENTITY, surname VARCHAR(256), givenName VARCHAR(256))",
]

var insertData = [
    "INSERT INTO car (type, name) VALUES('Ford', 'Mustang')",
    "INSERT INTO car (type, name) VALUES('Ford', 'Fiesta')",
    "INSERT INTO owner (surname,givenName) VALUES('Ford', 'Henry')"

]




var server = null;
describe("server async tests", function () {

    describe("asynchronized requests",function(){

        server = new Server();

        it("ping", function (done) {
            server.ping(function(err,res){
                assert.equal(true, res.duration > 0);
                done(err)
            })

        })


        it("register datasource", function (done) {
            server.dataSource(dsn,dsSpec,function(err,res){
                assert.equal(true, res.getId() > 0);
                done(err)
            });
        })


        it("get registered datasource", function (done) {
            server.dataSource(dsn,function(err,res){
                assert.equal(true, res.getId() > 0);
                done(err)
            });

        })

        it("create db", function (done) {
            var ds = server.dataSource(dsn);
            assert.equal(true, ds.getId() > 0);

            ds.executeBatch(createDBSchema,function(err,res){
                assert.equal(2, res.length);
                done(err)
            });

        })


        it("clear tables db", function (done) {
            var ds = server.dataSource(dsn);
            assert.equal(true, ds.getId() > 0);

            ds.execute('TRUNCATE TABLE car',function(err,res){
                assert.equal(1, res);


                ds.execute('TRUNCATE TABLE owner',function(err2,res2){
                    assert.equal(1, res2);
                    done(err2)
                });
            });


        })

        it("insert data", function (done) {
            var ds = server.dataSource(dsn);
            assert.equal(true, ds.getId() > 0);

            ds.executeBatch(insertData,function(err,res){
                assert.equal(3, res.length);
                done(err);
            });

        })




        it("update data", function (done) {
            var ds = server.dataSource(dsn);
            assert.equal(true, ds.getId() > 0);

            ds.update("INSERT INTO car (type, name) VALUES('Volvo', 'V70')",function(err,res){
                assert.equal(1, res);
                done(err);
            });

        })

        it("query data", function (done) {
            var ds = server.dataSource(dsn);
            assert.equal(true, ds.getId() > 0);

            ds.query("SELECT * FROM car",function(err,res){
                assert.equal(3, res.length);
                done(err);
            });

        })

        it("list catalogs", function (done) {
            var ds = server.dataSource(dsn);
            assert.equal(true, ds.getId() > 0);

            ds.listCatalogs(function(err,res){
                console.log(res);
                done(err);
            });

        })

        it("list tables", function (done) {
            var ds = server.dataSource(dsn);
            assert.equal(true, ds.getId() > 0);

            /*
            ds.listTables('PUBLIC');
            done();
            */

            ds.listTables('PUBLIC', function(err,res){
                console.log(res);
                done(err);
            });


        })

    })

})