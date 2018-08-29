const util = require('util');
var inherits = util.inherits
  , Connection = require('./connection')
  , _ = require("lodash");


var dataSourceTypes = {};

var DataSource = function (server, options) {
  options = options || {};

  this.options = options;
  this.dsn = options.name;
  this.type = options.type;
  this.server = server;

};

DataSource.get = function (type) {
  return dataSourceTypes[type];
};

DataSource.prototype.getId = function () {
  return this.options.id;
};

DataSource.prototype.getName = function () {
  return this.dsn;
};

DataSource.prototype.getType = function () {
  return this.type;
};

DataSource.prototype.register = function (fn) {
  var self = this;
  if (fn && util.isFunction(fn)) {
    this.server.command('ds', {method: 'register', spec: self.options}, function (err, response) {
      self.options = _.merge(self.options, response.spec);
      fn(err, self)
    });
  } else {
    var response = this.server.command('ds', {method: 'register', spec: self.options}, fn);
    this.options = _.merge(this.options, response.spec);
    return this;
  }

};

DataSource.prototype.unregister = function () {
};
DataSource.prototype.query = function (query) {
};
DataSource.prototype.listTables = function () {
};
DataSource.prototype.listCatalogs = function () {
};
DataSource.prototype.listDatabases = function () {
};


var JdbcDataSource = function (server, options) {
  options = options || {};
  DataSource.call(this, server, options)
};

dataSourceTypes['jdbc'] = JdbcDataSource;

inherits(JdbcDataSource, DataSource);


JdbcDataSource.prototype.execute = function (sql, fn) {
  var data = {
    dsn: this.getName(),
    query: {
      '@t': 'jdbc.q.exec',
      sql: sql
    }
  };

  if (fn && util.isFunction(fn)) {
    this.server.command('ds.query', data, function (err, res) {
      if (res.result.ret) {
        fn(err, res.result.data)
      } else {
        fn(err, res.result.affected)
      }
    });
  } else {
    var response = this.server.command('ds.query', data, fn);
    if (response.result.ret) {
      return response.result.data
    } else {
      return response.result.affected
    }
  }
};

JdbcDataSource.prototype.select = function (sql, fn) {
  return this.query(sql, fn);
};

JdbcDataSource.prototype.query = function (sql, fn) {
  var data = {
    dsn: this.getName(),
    query: {
      '@t': 'jdbc.q.select',
      sql: sql
    }
  };

  if (fn && util.isFunction(fn)) {
    this.server.command('ds.query', data, function (err, res) {
      fn(err, res.result.data)
    });
  } else {
    var response = this.server.command('ds.query', data, fn);
    return response.result.data;
  }
};

JdbcDataSource.prototype.update = function (sql, fn) {
  var data = {
    dsn: this.getName(),
    query: {
      '@t': 'jdbc.q.update',
      sql: sql
    }
  };

  if (fn && util.isFunction(fn)) {
    this.server.command('ds.query', data, function (err, res) {
      fn(err, res.result.affected)
    });
  } else {
    var response = this.server.command('ds.query', data, fn);
    return response.result.affected;
  }

};

JdbcDataSource.prototype.executeBatch = function (sqls, fn) {
  var data = {
    dsn: this.getName(),
    query: {
      '@t': 'jdbc.q.batch',
      sqls: sqls
    }
  };

  if (fn && util.isFunction(fn)) {
    this.server.command('ds.query', data, function (err, res) {
      fn(err, res.result.batchResults)
    });
  } else {
    var response = this.server.command('ds.query', data, fn);
    return response.result.batchResults;
  }
};

var _processCatalogResults = function (res) {
  var dbs = [];

  res.result.catalogs.forEach(function (x) {
    var db = {name: x, schemas: []};
    dbs.push(db);

    res.result.schemas.forEach(function (s) {
      if (s.catalog == x && s.schema) {
        db.schemas.push(s.schema)
      }
    })
  });

  return dbs;
};

JdbcDataSource.prototype.listCatalogs = function (fn) {
  var data = {
    dsn: this.getName(),
    query: {
      '@t': 'jdbc.q.schema'
    }
  };

  if (fn && util.isFunction(fn)) {
    this.server.command('ds.query', data, function (err, res) {
      var dbs = _processCatalogResults(res);
      fn(err, dbs)
    });
  } else {
    var response = this.server.command('ds.query', data, fn);
    return _processCatalogResults(response);
  }


};

JdbcDataSource.prototype.listDatabases = function (fn) {
  return this.listCatalogs(fn)
};

JdbcDataSource.prototype.listTables = function (selection, fn) {
  var data = {
    dsn: this.getName(),
    query: {
      '@t': 'jdbc.q.table',
    }
  };

  if (util.isString(selection)) {
    var split = selection.split('.');
    if (split.length == 2) {
      data.query.catalog = split[1];
      data.query.schema = split[0]
    } else {
      data.query.catalog = selection
    }

    // if dot split
  } else if (util.isObject(selection)) {
    data.query = merge(data.query, selection)
  }


  // TODO Catalog must be present
  if (fn && util.isFunction(fn)) {
    this.server.command('ds.query', data, function (err, res) {
      // TODO capture errors
      fn(err, res.result.tables)
    });
  } else {
    var response = this.server.command('ds.query', data, fn);
    return response.result.tables;
  }


};

DataSource.prototype.registerAsync = function () {
  return new Promise((resolve, reject) => {
    this.register((err, res) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    })
  })
};


JdbcDataSource.prototype.executeAsync = function (sql) {
  return new Promise((resolve, reject) => {
    this.execute(sql, (err, res) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    })
  })
};

JdbcDataSource.prototype.updateAsync = function (sql) {
  return new Promise((resolve, reject) => {
    this.update(sql, (err, res) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    })
  })
};


JdbcDataSource.prototype.queryAsync = function (sql) {
  return new Promise((resolve, reject) => {
    this.query(sql, (err, res) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    })
  })
};


JdbcDataSource.prototype.selectAsync = function (sql) {
  return new Promise((resolve, reject) => {
    this.select(sql, (err, res) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    })
  })
};

JdbcDataSource.prototype.listCatalogsAsync = function () {
  return new Promise((resolve, reject) => {
    this.listCatalogs((err, res) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    })
  })
};


JdbcDataSource.prototype.listTablesAsync = function (selection) {
  return new Promise((resolve, reject) => {
    this.listTables(selection, (err, res) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    })
  })
};


module.exports = DataSource;
