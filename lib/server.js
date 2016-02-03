var inherits = require('util').inherits
    , EventEmitter = require('events').EventEmitter
    , Connection = require('./connection')
    , Actions = require('./actions')
    , DataSource = require('./datasource')
    , f = require('util').format
    , clone = require('clone')
    , merge = require('merge')

var Server = function(options){
    options = options || {}

    this.options = options
    this.options.host = options.host || 'localhost'
    this.options.port = options.port ||  8118

    // Internals
    this.dataSources = {}

}

Server.prototype.command = function(ns, data, obj, fn){
    var opts = {}
    var t = new Actions.Transport(ns, data);

    // currently only synced!
    var connection = this.connection(opts);
    connection.connect()
    var response = null
    try{
        response = connection.write(t.toBin());
    }catch(e){
        console.trace(e)
        throw e;
    }

    connection.destroy();

    if(response.data.errors){
        console.log(response.errors);
        throw new Error(response.errors)
    }
    return response.data;
}

Server.prototype.connection = function(options){
    var _options = clone(this.options)
    if(options){
        _options = merge(_options,options)
    }
    var connection = new Connection(_options);
    return connection;
}

Server.prototype.ping = function(){
    return this.command('sys.ping',{time:new Date()});
}

Server.prototype.hasDataSource = function(name){
    return name in this.dataSources
}

Server.prototype.dataSource = function(name, options, fn){

    if(!name) throw new Error('name must be set')

    if(this.hasDataSource(name)){
        return this.dataSources[name];
    }

    var self = this;

    // is not registered
    if(options){
        if(!options.type){
            throw new Error('datasource not present, type must be specified')
        }
        var dsClass = DataSource.get(options.type)
        if(!dsClass){
            throw new Error('no class found for ' + options.type)
        }

        options['name'] = name;
        this.dataSources[name] = new dsClass(self, options);

        if(fn && typeof fn == 'function' ){
            // async
            this.dataSources[name].register(fn)
        }else{
            // sync
            return this.dataSources[name].register();
        }
    }else{
        throw new Error('datasource not present, options must be set')
    }
}

module.exports = Server
