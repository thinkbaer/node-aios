import * as BSON from 'bson';


let _requestId = 0;

let PING = 'sys.ping';
let DATASOURCE_REGISTER = 'ds';
let DATASOURCE_QUERY = 'ds.query';


let bsonTypes = [BSON.Long, BSON.ObjectID, BSON.Binary,
  BSON.Code, BSON.DBRef, BSON.Symbol, BSON.Double,
  BSON.Timestamp, BSON.MaxKey, BSON.MinKey];
let bsonInstance: any = null;

let Transport = function (ns: any, data: any, options: any) {
  // Basic options
  options = options || {};
  this.header = {ns: null, req: true, op: null, rid: _requestId++}; // ,flags:b.Binary.fromInt(0)
  this.header['ns'] = ns;
  this.data = data || {};

  // @ts-ignore
  bsonInstance = bsonInstance == null ? new BSON(bsonTypes) : bsonInstance;
  this.bson = options.bson ? options.bson : bsonInstance;

  // BSON settings
  this.checkKeys = false;
  this.asBuffer = true;
  this.serializeFunctions = false;
};

Transport.prototype.toBin = function () {
  let self = this;
  let buffers = [];
  let projection = null;

  // Serialize the query
  let headerBson = this.bson.serialize(self.header
    , this.checkKeys
    , true
    , this.serializeFunctions);


  // Serialize the query
  let dataBson = this.bson.serialize(this.data
    , this.checkKeys
    , true
    , this.serializeFunctions);


  // Total message size
  let totalLength = headerBson.length + dataBson.length;

  // Set up the index
  let index = 4;

  let header = new Buffer(8);

  // Write total document length
  header[0] = (totalLength >> 24) & 0xff;
  header[1] = (totalLength >> 16) & 0xff;
  header[2] = (totalLength >> 8) & 0xff;
  header[3] = (totalLength) & 0xff;

  let headerLength = headerBson.length;
  header[index + 0] = (headerLength >> 24) & 0xff;
  header[index + 1] = (headerLength >> 16) & 0xff;
  header[index + 2] = (headerLength >> 8) & 0xff;
  header[index + 3] = (headerLength) & 0xff;

  let buffer = new Buffer(8 + totalLength);
  header.copy(buffer);
  headerBson.copy(buffer, 8);
  dataBson.copy(buffer, 8 + headerLength);

  // Return the buffers
  return buffer;
};

let Response = function (data: any, opts: any) {
  opts = opts || {promoteLongs: true};
  this.parsed = false;

  // @ts-ignore
  bsonInstance = bsonInstance == null ? new BSON(bsonTypes) : bsonInstance;
  this.bson = opts.bson ? opts.bson : bsonInstance;
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


  this.promoteLongs = typeof opts.promoteLongs == 'boolean' ? opts.promoteLongs : true;
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

  let _options = {promoteLongs: this.opts.promoteLongs};
  this.header = this.bson.deserialize(this.raw.slice(8, 8 + this.headerLength), _options);
  this.data = this.bson.deserialize(this.raw.slice(8 + this.headerLength), _options);
  // Set parsed
  this.parsed = true;
};


let PingRequest = function () {
  this.time = new Date();
};

module.exports = {
  Response: Response,
  Transport: Transport,
  PingRequest: PingRequest
};
