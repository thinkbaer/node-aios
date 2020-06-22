import {inherits} from 'util';
import * as _ from 'lodash';

let dataSourceTypes = {};

export function DataSource(server: any, options: any) {
  options = options || {};

  this.options = options;
  this.dsn = options.name;
  this.type = options.type;
  this.server = server;

};

// @ts-ignore
DataSource.get = function (type: any) {
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

DataSource.prototype.register = function (fn: any) {
  let self = this;
  if (fn && _.isFunction(fn)) {
    this.server.command('ds', {method: 'register', spec: self.options}, function (err: any, response: any) {
      self.options = _.merge(self.options, response.spec);
      fn(err, self);
    });
  } else {
    let response = this.server.command('ds', {method: 'register', spec: self.options}, fn);
    this.options = _.merge(this.options, response.spec);
    return this;
  }

};

DataSource.prototype.unregister = function () {
};
DataSource.prototype.query = function (query: any) {
};
DataSource.prototype.listTables = function () {
};
DataSource.prototype.listCatalogs = function () {
};
DataSource.prototype.listDatabases = function () {
};


let JdbcDataSource = function (server: any, options: any) {
  options = options || {};
  DataSource.call(this, server, options);
};

dataSourceTypes['jdbc'] = JdbcDataSource;

inherits(JdbcDataSource, DataSource);


JdbcDataSource.prototype.execute = function (sql: any, fn: any) {
  let data = {
    dsn: this.getName(),
    query: {
      '@t': 'jdbc.q.exec',
      sql: sql
    }
  };

  if (fn && _.isFunction(fn)) {
    this.server.command('ds.query', data, function (err: any, res: any) {
      if (res && res.result && res.result['@t']) {
        delete res.result['@t'];
      }
      if (err) {
        fn(err);
      } else {
        fn(null, res.result);
      }
    });
  } else {
    let response = this.server.command('ds.query', data, fn);
    delete response.result['@t'];
    return response.result;
  }
};

JdbcDataSource.prototype.select = function (sql: any, fn: any) {
  return this.query(sql, fn);
};

JdbcDataSource.prototype.query = function (sql: any, fn: any) {
  let data = {
    dsn: this.getName(),
    query: {
      '@t': 'jdbc.q.select',
      sql: sql
    }
  };

  if (fn && _.isFunction(fn)) {
    this.server.command('ds.query', data, function (err: any, res: any) {
      if (err) {
        fn(err);
      } else {
        fn(null, res.result ? res.result.data : []);
      }
    });
  } else {
    let response = this.server.command('ds.query', data, fn);
    return response.result.data;
  }
};

JdbcDataSource.prototype.update = function (sql: any, fn: any) {
  let data = {
    dsn: this.getName(),
    query: {
      '@t': 'jdbc.q.update',
      sql: sql
    }
  };

  if (fn && _.isFunction(fn)) {
    this.server.command('ds.query', data, function (err: any, res: any) {
      if (res && res.result && res.result['@t']) {
        delete res.result['@t'];
      }
      if (err) {
        fn(err);
      } else {
        fn(null, res.result);
      }
    });
  } else {
    let response = this.server.command('ds.query', data, fn);
    delete response.result['@t'];
    return response.result;
  }

};

JdbcDataSource.prototype.executeBatch = function (sqls: any, fn: any) {
  let data = {
    dsn: this.getName(),
    query: {
      '@t': 'jdbc.q.batch',
      sqls: sqls
    }
  };

  if (fn && _.isFunction(fn)) {
    this.server.command('ds.query', data, function (err: any, res: any) {
      if (res && res.result && res.result['@t']) {
        delete res.result['@t'];
      }
      if (err) {
        fn(err);
      } else {
        fn(null, res.result);
      }
    });
  } else {
    let response = this.server.command('ds.query', data, fn);
    delete response.result['@t'];
    return response.result;
  }
};

let _processCatalogResults = function (res: any) {
  let dbs: any = [];

  res.result.catalogs.forEach(function (x: any) {
    let db: any = {name: x, schemas: []};
    dbs.push(db);

    res.result.schemas.forEach(function (s: any) {
      if (s.catalog == x && s.schema) {
        db.schemas.push(s.schema);
      }
    });
  });

  return dbs;
};

JdbcDataSource.prototype.listCatalogs = function (fn: any) {
  let data = {
    dsn: this.getName(),
    query: {
      '@t': 'jdbc.q.schema'
    }
  };

  if (fn && _.isFunction(fn)) {
    this.server.command('ds.query', data, function (err: any, res: any) {
      let dbs = _processCatalogResults(res);
      fn(err, dbs);
    });
  } else {
    let response = this.server.command('ds.query', data, fn);
    return _processCatalogResults(response);
  }


};

JdbcDataSource.prototype.listDatabases = function (fn: any) {
  return this.listCatalogs(fn);
};

JdbcDataSource.prototype.listTables = function (selection: any, fn: any) {
  let data: any = {
    dsn: this.getName(),
    query: {
      '@t': 'jdbc.q.table',
    }
  };

  if (_.isString(selection)) {
    let split = selection.split('.');
    if (split.length == 2) {
      data.query.catalog = split[1];
      data.query.schema = split[0];
    } else {
      data.query.catalog = selection;
    }

    // if dot split
  } else if (_.isObject(selection)) {
    data.query = _.merge(data.query, selection);
  }


  // TODO Catalog must be present
  if (fn && _.isFunction(fn)) {
    this.server.command('ds.query', data, function (err: any, res: any) {
      // TODO capture errors
      if (err) {
        fn(err);
      } else {
        fn(null, res.result.tables);
      }

    });
  } else {
    let response = this.server.command('ds.query', data, fn);
    return response.result.tables;
  }


};

DataSource.prototype.registerAsync = function () {
  return new Promise((resolve, reject) => {
    this.register((err: any, res: any) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    });
  });
};


JdbcDataSource.prototype.executeAsync = function (sql: any) {
  return new Promise((resolve, reject) => {
    this.execute(sql, (err: any, res: any) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    });
  });
};

JdbcDataSource.prototype.updateAsync = function (sql: any) {
  return new Promise((resolve, reject) => {
    this.update(sql, (err: any, res: any) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    });
  });
};


JdbcDataSource.prototype.queryAsync = function (sql: any) {
  return new Promise((resolve, reject) => {
    this.query(sql, (err: any, res: any) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    });
  });
};


JdbcDataSource.prototype.selectAsync = function (sql: any) {
  return new Promise((resolve, reject) => {
    this.select(sql, (err: any, res: any) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    });
  });
};

JdbcDataSource.prototype.listCatalogsAsync = function () {
  return new Promise((resolve, reject) => {
    this.listCatalogs((err: any, res: any) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    });
  });
};


JdbcDataSource.prototype.listTablesAsync = function (selection: any) {
  return new Promise((resolve, reject) => {
    this.listTables(selection, (err: any, res: any) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    });
  });
};

