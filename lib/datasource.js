var inherits = require('util').inherits
    , EventEmitter = require('events').EventEmitter
    , Connection = require('./connection')
    , Actions = require('./actions')
    , f = require('util').format
    , clone = require('clone')
    , merge = require('merge')



var dataSourceTypes = {}

var DataSource = function(server, options){
    options = options || {}

    this.options = options
    this.dsn = options.name
    this.type = options.type
    this.server = server;

}

DataSource.get = function(type){
    return dataSourceTypes[type];
}

DataSource.prototype.getId = function(){
    return this.options.id;
}

DataSource.prototype.getName = function(){
    return this.dsn;
}

DataSource.prototype.getType = function(){
    return this.type;
}

DataSource.prototype.register = function(fn){
    var self = this;
    var response = this.server.command('ds',{method:'register',spec:self.options},self,fn);
    this.options = merge(this.options,response.spec)
    return this;
}

DataSource.prototype.unregister = function(){

}

DataSource.prototype.query = function(query){

}


var JdbcDataSource = function(server, options){
    options = options || {}
    DataSource.call(this,server, options)
}

dataSourceTypes['jdbc'] = JdbcDataSource

inherits(JdbcDataSource,DataSource)


JdbcDataSource.prototype.execute = function(sql,fn){
    var data = {
        dsn: this.getName(),
        query: {
            '@t':'jdbc.q.exec',
            sql:sql
        }
    }

    var self = this;
    var response = this.server.command('ds.query',data,self,fn);

    if(response.result.ret){
        return response.result.data
    }else{
        return response.result.affected
    }

}

JdbcDataSource.prototype.select = function(sql,fn){
    return this.query(sql,fn);
}

JdbcDataSource.prototype.query = function(sql,fn){
    var data = {
        dsn: this.getName(),
        query: {
            '@t':'jdbc.q.select',
            sql:sql
        }
    }
    var self = this;
    var response = this.server.command('ds.query',data,self,fn);

    return response.result.data;

}

JdbcDataSource.prototype.update = function(sql,fn){
    var data = {
        dsn: this.getName(),
        query: {
            '@t':'jdbc.q.update',
            sql:sql
        }
    }
    var self = this;
    var response = this.server.command('ds.query',data,self,fn);

    return response.result.affected;

}

JdbcDataSource.prototype.executeBatch = function(sqls,fn){
    var data = {
        dsn: this.getName(),
        query: {
            '@t':'jdbc.q.batch',
            sqls:sqls
        }
    }

    var self = this;
    var response = this.server.command('ds.query',data,self,fn);


    return response.result.batchResults;
}



module.exports = DataSource