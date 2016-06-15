const util = require('util');
var inherits = util.inherits
    , Connection = require('./connection')
    , merge = require('merge')
    , Promise = require('bluebird')




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
    if(fn && util.isFunction(fn)){
        this.server.command('ds',{method:'register',spec:self.options},function(err,response){
            self.options = merge(self.options,response.spec)
            fn(err,self)
        });
    }else{
        var response = this.server.command('ds',{method:'register',spec:self.options},fn);
        this.options = merge(this.options,response.spec)
        return this;
    }

}

DataSource.prototype.unregister = function(){}
DataSource.prototype.query = function(query){}
DataSource.prototype.listTables = function(){}
DataSource.prototype.listCatalogs = function(){}


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

    if(fn && util.isFunction(fn)){
        this.server.command('ds.query',data,function(err,res){
            if(res.result.ret){
                fn(err, res.result.data)
            }else{
                fn(err, res.result.affected)
            }
        });
    }else{
        var response = this.server.command('ds.query',data,fn);
        if(response.result.ret){
            return response.result.data
        }else{
            return response.result.affected
        }
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

    if(fn && util.isFunction(fn)){
        this.server.command('ds.query',data,function(err,res){
            fn(err, res.result.data)
        });
    }else{
        var response = this.server.command('ds.query',data,fn);
        return response.result.data;
    }
}

JdbcDataSource.prototype.update = function(sql,fn){
    var data = {
        dsn: this.getName(),
        query: {
            '@t':'jdbc.q.update',
            sql:sql
        }
    }

    if(fn && util.isFunction(fn)){
        this.server.command('ds.query',data,function(err,res){
            fn(err, res.result.affected)
        });
    }else{
        var response = this.server.command('ds.query',data,fn);
        return response.result.affected;
    }

}

JdbcDataSource.prototype.executeBatch = function(sqls,fn){
    var data = {
        dsn: this.getName(),
        query: {
            '@t':'jdbc.q.batch',
            sqls:sqls
        }
    }

    if(fn && util.isFunction(fn)){
        this.server.command('ds.query',data,function(err,res){
            fn(err, res.result.batchResults)
        });
    }else{
        var response = this.server.command('ds.query',data,fn);
        return response.result.batchResults;
    }
}

var _processCatalogResults = function(res){
    var dbs = []

    res.result.catalogs.forEach(function(x){
        var db = {name: x, schemas:[]}
        dbs.push(db)

        res.result.schemas.forEach(function(s){
            if(s.catalog == x && s.schema){
                db.schemas.push(s.schema)
            }
        })
    })

    return dbs;
}

JdbcDataSource.prototype.listCatalogs = function(fn){
    var data = {
        dsn: this.getName(),
        query: {
            '@t':'jdbc.q.schema'
        }
    }

    if(fn && util.isFunction(fn)){
        this.server.command('ds.query',data,function(err,res){
            var dbs = _processCatalogResults(res)
            fn(err, dbs)
        });
    }else{
        var response = this.server.command('ds.query',data,fn);
        return _processCatalogResults(response);
    }


}

JdbcDataSource.prototype.listDatabases = function(fn){
    return this.listCatalogs(fn)
}

JdbcDataSource.prototype.listTables = function(selection, fn){
    var data = {
        dsn: this.getName(),
        query: {
            '@t':'jdbc.q.table',
        }
    }

    if(util.isString(selection)){
        var split = selection.split('.')
        if(split.length == 2){
            data.query.catalog = split[1]
            data.query.schema = split[0]
        }else{
            data.query.catalog = selection
        }

        // if dot split
    }else if(util.isObject(selection)){
        data.query = merge(data.query,selection)
    }


    // TODO Catalog must be present
    if(fn && util.isFunction(fn)){
        this.server.command('ds.query',data,function(err,res){
            // TODO capture errors
            fn(err, res.result.tables)
        });
    }else{
        var response = this.server.command('ds.query',data,fn);
        return response.result.tables;
    }


}

DataSource.prototype.registerAsync = Promise.promisify(DataSource.prototype.register)
JdbcDataSource.prototype.executeAsync = Promise.promisify(JdbcDataSource.prototype.execute)
JdbcDataSource.prototype.updateAsync = Promise.promisify(JdbcDataSource.prototype.update)
JdbcDataSource.prototype.queryAsync = Promise.promisify(JdbcDataSource.prototype.query)
JdbcDataSource.prototype.selectAsync = Promise.promisify(JdbcDataSource.prototype.select)
JdbcDataSource.prototype.listCatalogsAsync = Promise.promisify(JdbcDataSource.prototype.listCatalogs)
JdbcDataSource.prototype.listTablesAsync = Promise.promisify(JdbcDataSource.prototype.listTables)


module.exports = DataSource
