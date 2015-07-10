# Aios connector

## Usage

```
var aios = require('aios');

var server = new aios.Server({host:'localhost',port:8118});

server.ping(function(response){

});

// Sync mode
var response = server.ping();


// get or register
server.ds(name, {type:'jdbc',url:'...',driver:'...'}, function(ds){
    // returns a jdbc datasource
    ds.execute(..., [fn])

    ds.update(...,[fn])

    ds.batch(...,[fn])

    ds.query("SELECT * FROM car",[fn])
})

```