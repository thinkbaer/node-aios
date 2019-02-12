const util = require('util');
var Connection = require('./connection')
  , Actions = require('./actions')
  , DataSource = require('./datasource')
  , _ = require("lodash");

var Server = function (options) {
  options = options || {};

  this.options = options;
  this.options.host = options.host || 'localhost';
  this.options.port = options.port || 8118;

  // Internals
  this.dataSources = {}

};

Server.prototype.command = function (ns, data, fn) {
  var isSync = true;
  if (fn && util.isFunction(fn)) {
    isSync = false
  }

  if (isSync) {
    return this._cmdSync(ns, data)
  } else {
    this._cmdAsync(ns, data, fn)
  }
};


Server.prototype._cmdAsync = function (ns, data, fn) {
  var opts = {};
  var t = new Actions.Transport(ns, data);

  var connection = this.connection(opts);
  connection.connect(function (err, conn) {
    if (!err) {
      conn.write(t.toBin(), function (err, res) {
        connection.destroy(function () {
          if (res.data.errors) {
            fn(new Error(JSON.stringify(res.data)))
          } else if (err) {
            fn(err)
          } else {
            fn(null, res.data)
          }
        });
      })
    } else {
      connection.destroy(function () {
        fn(err)
      });
    }
  })

};

Server.prototype._cmdSync = function (ns, data) {
  var opts = {};
  var t = new Actions.Transport(ns, data);

  // currently only synced!
  var connection = this.connection(opts);
  connection.connect();
  var response = null;
  try {
    response = connection.write(t.toBin());
  } catch (e) {
    console.trace(e);
    throw e;
  }

  connection.destroy();

  if (response.data.errors) {
    console.log(response.data);
    throw new Error(response.data.errors)
  }
  return response.data;
};

Server.prototype.connection = function (options) {
  var _options = _.clone(this.options);
  if (options) {
    _options = _.merge(_options, options)
  }
  var connection = new Connection(_options);
  return connection;
};

Server.prototype.ping = function (fn) {
  return this.command('sys.ping', {time: new Date()}, fn);
};

Server.prototype.hasDataSource = function (name) {
  return name in this.dataSources
};

Server.prototype.dataSource = function (name, options, fn) {

  var async = false;
  if (fn && util.isFunction(fn)) {
    async = true
  }

  if (options && util.isFunction(options)) {
    async = true;
    fn = options;
    options = null
  }

  if (!name) throw new Error('name must be set');

  if (this.hasDataSource(name)) {

    if (async) {
      return fn(null, this.dataSources[name])
    } else {
      return this.dataSources[name];
    }

  }

  var self = this;

  // is not registered
  if (options) {
    if (!options.type) {
      throw new Error('datasource not present, type must be specified')
    }
    var dsClass = DataSource.get(options.type);
    if (!dsClass) {
      throw new Error('no class found for ' + options.type)
    }

    options['name'] = name;
    this.dataSources[name] = new dsClass(self, options);

    if (async) {
      // async
      this.dataSources[name].register(fn)
    } else {
      // sync
      return this.dataSources[name].register();
    }
  } else {
    throw new Error('datasource not present, options must be set')
  }
};

Server.prototype.pingAsync = function () {
  return new Promise((resolve, reject) => {
    this.ping((err, res) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    })
  })
};


Server.prototype.commandAsync = function (ns, data) {
  return new Promise((resolve, reject) => {
    this.command(ns, data, (err, res) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    })
  })
};
Server.prototype.dataSourceAsync = function (name, options) {
  return new Promise((resolve, reject) => {
    this.dataSource(name, options, (err, res) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    })
  })
};

module.exports = Server;
