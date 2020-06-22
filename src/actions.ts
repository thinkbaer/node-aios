// import * as BSON from 'bson';


import {serialize, deserialize} from 'bson';

let _requestId = 0;

const PING = 'sys.ping';
const DATASOURCE_REGISTER = 'ds';
const DATASOURCE_QUERY = 'ds.query';

//
// const bsonTypes = [BSON.Long, BSON.ObjectID, BSON.Binary,
//   BSON.Code, BSON.DBRef, BSON.Symbol, BSON.Double,
//   BSON.Timestamp, BSON.MaxKey, BSON.MinKey];
// let bsonInstance: any = null;

const Transport = function (ns: any, data: any, options: any) {
  // Basic options
  options = options || {};
  this.header = {ns: null, req: true, op: null, rid: _requestId++}; // ,flags:b.Binary.fromInt(0)
  this.header['ns'] = ns;
  this.data = data || {};

  // // @ts-ignore
  // bsonInstance = bsonInstance == null ? new BSON(bsonTypes) : bsonInstance;
  // this.bson = options.bson ? options.bson : bsonInstance;

  // BSON settings
  this.checkKeys = false;
  this.asBuffer = true;
  this.serializeFunctions = false;
};

Transport.prototype.toBin = function () {
  const self = this;
  // const buffers = [];
  // const projection = null;

  // // Serialize the query
  // const headerBson = this.bson.serialize(self.header
  //   , this.checkKeys
  //   , true
  //   , this.serializeFunctions);
  //
  //
  // // Serialize the query
  // const dataBson = this.bson.serialize(this.data
  //   , this.checkKeys
  //   , true
  //   , this.serializeFunctions);

  const headerBson = serialize(self.header, {
    serializeFunctions: this.serializeFunctions,
    checkKeys: this.checkKeys,
    ignoreUndefined: true
  });


  // Serialize the query
  const dataBson = serialize(self.data, {
    serializeFunctions: this.serializeFunctions,
    checkKeys: this.checkKeys,
    ignoreUndefined: true
  });
  // Total message size
  const totalLength = headerBson.length + dataBson.length;

  // Set up the index
  const index = 4;

  const header = new Buffer(8);

  // Write total document length
  header[0] = (totalLength >> 24) & 0xff;
  header[1] = (totalLength >> 16) & 0xff;
  header[2] = (totalLength >> 8) & 0xff;
  header[3] = (totalLength) & 0xff;

  const headerLength = headerBson.length;
  header[index + 0] = (headerLength >> 24) & 0xff;
  header[index + 1] = (headerLength >> 16) & 0xff;
  header[index + 2] = (headerLength >> 8) & 0xff;
  header[index + 3] = (headerLength) & 0xff;

  const buffer = new Buffer(8 + totalLength);
  header.copy(buffer);
  headerBson.copy(buffer, 8);
  dataBson.copy(buffer, 8 + headerLength);

  // Return the buffers
  return buffer;
};

const Response = function (data: any, opts: any) {
  opts = opts || {promoteLongs: true};
  this.parsed = false;

  // @ts-ignore
  // bsonInstance = bsonInstance == null ? new BSON(bsonTypes) : bsonInstance;
  // this.bson = opts.bson ? opts.bson : bsonInstance;
  //
  // Parse Header
  //
  this.index = 0;
  this.raw = data;
  this.data = null;
  this.header = null;

  this.opts = opts;

  // Read the total length
  // this.totalLength = data[this.index] | data[this.index + 1] << 8 | data[this.index + 2] << 16 | data[this.index + 3] << 24;
  this.totalLength = data[this.index] << 24 | data[this.index + 1] << 16 | data[this.index + 2] << 8 | data[this.index + 3];
  this.index = this.index + 4;

  // Read header length
  this.headerLength = data[this.index] << 24 | data[this.index + 1] << 16 | data[this.index + 2] << 8 | data[this.index + 3];


  this.promoteLongs = typeof opts.promoteLongs === 'boolean' ? opts.promoteLongs : true;
};

Response.prototype.isParsed = function () {
  return this.parsed;
};

Response.prototype.parse = function (options: any) {
  // Don't parse again if not needed
  if (this.parsed) {
    return;
  }
  options = options || {};

  // const _options = {promoteLongs: this.opts.promoteLongs};
  const _options: any = {promoteLongs: this.opts.promoteLongs};
  // this.header = this.bson.deserialize(this.raw.slice(8, 8 + this.headerLength), _options);
  // this.data = this.bson.deserialize(this.raw.slice(8 + this.headerLength), _options);
  const _buffer = Buffer.from(this.raw);
  const _headerBuffer = _buffer.slice(8, 8 + this.headerLength);
  const _dataBuffer = _buffer.slice(8 + this.headerLength);
  const _header = deserialize(_headerBuffer, _options);
  const _data = deserialize(_dataBuffer, _options);
  this.header = _header;
  this.data = _data;
  // Set parsed
  this.parsed = true;
};


const PingRequest = function () {
  this.time = new Date();
};

module.exports = {
  Response: Response,
  Transport: Transport,
  PingRequest: PingRequest
};
