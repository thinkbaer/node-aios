import {format as f, inherits} from 'util';
import * as net from 'net';
import {EventEmitter} from 'events';
import {Utils} from './utils';
import * as _ from 'lodash';
import * as actions from './actions';

/**
 * Implements Socket Connection to Aios Server
 */


let _id = 0;

// @ts-ignore
const Response = actions.Response;

export function Connection(options: {
  debug?: any;
  messageHandler?: any; maxBsonMessageSize?: any;
  port?: any; host?: any; keepAlive?: any; keepAliveInitialDelay?: any; noDelay?: any; connectionTimeout?: any; socketTimeout?: any;
}) {

  options = options || {};

  // Set empty if no options passed
  this.options = options;
  // Identification information
  this.id = _id++;
  this.debug = options.debug || false;


  // Message handler
  this.messageHandler = options.messageHandler ? options.messageHandler : defaultMessageHandler;

  // Max JSON message size
  this.maxBsonMessageSize = options.maxBsonMessageSize || (1024 * 1024 * 16 * 4);


  // Default options
  this.port = options.port || 8118;
  this.host = options.host || 'localhost';
  this.keepAlive = typeof options.keepAlive === 'boolean' ? options.keepAlive : true;
  this.keepAliveInitialDelay = options.keepAliveInitialDelay || 0;
  this.noDelay = typeof options.noDelay === 'boolean' ? options.noDelay : true;
  this.connectionTimeout = options.connectionTimeout || 60000;
  this.socketTimeout = options.socketTimeout || 60000;

  // Check if we have a domain socket
  this.domainSocket = this.host.indexOf('\/') !== -1;

  // Internal state
  this.connection = null;
  this.isSync = false;
  this.callbacks = {};
}


const defaultMessageHandler = function (buffer: any) {
  const response = new Response(buffer);
  response.parse();
  return response;
};


//
// Connection handlers
const errorHandler = function (sock: { getConnection: () => any; handleError: (arg0: any) => void; }) {
  return function (err: any) {
    // Debug information
    const conn = sock.getConnection();
    if (conn.debug) {
      console.trace(f('connection %s for [%s:%s] errored out with [%s]', conn.id, conn.host, conn.port, JSON.stringify(err)));
    }
    // Emit the error
    sock.handleError(err);

  };
};

const timeoutHandler = function (self: { getConnection: () => any; handleTimeout: () => void; }) {
  return function () {
    // Debug information
    const conn = self.getConnection();
    if (conn.debug) {
      console.trace(f('connection %s for [%s:%s] timed out', conn.id, conn.host, conn.port));
    }
    // Emit timeout error
    self.handleTimeout();
  };
};

const closeHandler = function (self: { getConnection: () => any; handleClose: (arg0: any) => void; }) {
  return function (hadError: any) {
    // Debug information
    const conn = self.getConnection();
    if (conn.debug) {
      console.trace(f('connection %s with for [%s:%s] closed', conn.id, conn.host, conn.port));
    }
    // Emit close event
    self.handleClose(hadError);
  };
};

const dataHandler = function (self: any) {
  return function (data: any) {
    const maxBsonMessageSize = self.wrapper.maxBsonMessageSize;
    // Parse until we are done with the data
    while (data.length > 0) {
      // If we still have bytes to read on the current message
      if (self.bytesRead > 0 && self.sizeOfMessage > 0) {
        // Calculate the amount of remaining bytes
        const remainingBytesToRead = self.sizeOfMessage - self.bytesRead;
        // Check if the current chunk contains the rest of the message
        if (remainingBytesToRead > data.length) {
          // Copy the new data into the exiting buffer (should have been allocated when we know the message size)
          data.copy(self.buffer, self.bytesRead);
          // Adjust the number of bytes read so it point to the correct index in the buffer
          self.bytesRead = self.bytesRead + data.length;

          // Reset state of buffer
          data = new Buffer(0);
        } else {
          // Copy the missing part of the data into our current buffer
          data.copy(self.buffer, self.bytesRead, 0, remainingBytesToRead);
          // Slice the overflow into a new buffer that we will then re-parse
          data = data.slice(remainingBytesToRead);

          // Emit current complete message
          try {
            const emitBuffer = self.buffer;
            // Reset state of buffer
            self.buffer = null;
            self.sizeOfMessage = 0;

            self.bytesRead = 0;
            self.stubBuffer = null;
            // Emit the buffer
            self.handleData(emitBuffer);

          } catch (err) {
            const errorObject = {
              err: 'socketHandler',
              trace: err,
              bin: self.buffer,
              parseState: {
                sizeOfMessage: self.sizeOfMessage,
                bytesRead: self.bytesRead,
                stubBuffer: self.stubBuffer
              }
            };
            // We got a parse Error fire it off then keep going
            self.handleParseError(errorObject);
          }
        }
      } else {
        // Stub buffer is kept in case we don't get enough bytes to determine the
        // size of the message (< 8 bytes)
        if (self.stubBuffer != null && self.stubBuffer.length > 0) {
          // If we have enough bytes to determine the message size let's do it
          if (self.stubBuffer.length + data.length > 4) {
            // Prepad the data
            const newData = new Buffer(self.stubBuffer.length + data.length);
            self.stubBuffer.copy(newData, 0);
            data.copy(newData, self.stubBuffer.length);
            // Reassign for parsing
            data = newData;

            // Reset state of buffer
            self.buffer = null;
            self.sizeOfMessage = 0;
            self.bytesRead = 0;
            self.stubBuffer = null;

          } else {

            // Add the the bytes to the stub buffer
            const newStubBuffer = new Buffer(self.stubBuffer.length + data.length);
            // Copy existing stub buffer
            self.stubBuffer.copy(newStubBuffer, 0);
            // Copy missing part of the data
            data.copy(newStubBuffer, self.stubBuffer.length);
            // Exit parsing loop
            data = new Buffer(0);
          }
        } else {
          if (data.length > 8) {
            // Retrieve the message size
            // var totalLength = data.readUInt32LE(0);
            // var totalLength = data[0] | data[1] << 8 | data[2] << 16 | data[3] << 24;
            // var totalLength = data[0]  | data[1] << 8 | data[2] << 16 | data[3] << 24;
            const totalLength = Utils.encodeTotalLength(data);

            const sizeOfMessage = totalLength + 8;
            // If we have a negative totalLength emit error and return
            if (sizeOfMessage < 0 || sizeOfMessage > maxBsonMessageSize) {
              const errorObject = {
                err: 'socketHandler',
                trace: '',
                bin: self.buffer,
                parseState: {
                  sizeOfMessage: sizeOfMessage,
                  bytesRead: self.bytesRead,
                  stubBuffer: self.stubBuffer
                }
              };
              // We got a parse Error fire it off then keep going
              self.handleParseError(errorObject);

              return;
            }

            // Ensure that the size of message is larger than 0 and less than the max allowed
            if (sizeOfMessage > 8 && sizeOfMessage < maxBsonMessageSize && sizeOfMessage > data.length) {
              self.buffer = new Buffer(sizeOfMessage);
              // Copy all the data into the buffer
              data.copy(self.buffer, 0);
              // Update bytes read
              self.bytesRead = data.length;
              // Update totalLength
              self.sizeOfMessage = sizeOfMessage;
              // Ensure stub buffer is null
              self.stubBuffer = null;
              // Exit parsing loop
              data = new Buffer(0);

            } else if (sizeOfMessage > 8 && sizeOfMessage < maxBsonMessageSize && sizeOfMessage === data.length) {
              try {
                const emitBuffer = data;
                // Reset state of buffer
                self.buffer = null;
                self.sizeOfMessage = 0;
                self.bytesRead = 0;
                self.stubBuffer = null;
                // Exit parsing loop
                data = new Buffer(0);
                // Emit the message
                self.handleData(emitBuffer);

              } catch (err) {
                const errorObject = {
                  err: 'socketHandler',
                  trace: err,
                  bin: self.buffer,
                  parseState: {
                    sizeOfMessage: sizeOfMessage,
                    bytesRead: self.bytesRead,
                    stubBuffer: self.stubBuffer
                  }
                };
                // We got a parse Error fire it off then keep going
                self.handleParseError(errorObject);
              }
            } else if (sizeOfMessage <= 8 || sizeOfMessage > maxBsonMessageSize) {
              const errorObject: any = {
                err: 'socketHandler',
                trace: null,
                bin: data,
                parseState: {
                  sizeOfMessage: sizeOfMessage,
                  bytesRead: 0,
                  buffer: null,
                  stubBuffer: null
                }
              };
              // We got a parse Error fire it off then keep going
              self.handleParseError(errorObject);
              // Clear out the state of the parser
              self.buffer = null;
              self.bytesRead = 0;
              self.stubBuffer = null;
              // Exit parsing loop
              data = new Buffer(0);
            } else {
              const emitBuffer = data.slice(0, sizeOfMessage);
              // Reset state of buffer
              self.buffer = null;
              self.bytesRead = 0;
              self.stubBuffer = null;
              // Copy rest of message
              data = data.slice(sizeOfMessage);
              // Emit the message
              self.handleData(emitBuffer);
            }
          } else {
            // Create a buffer that contains the space for the non-complete message
            self.stubBuffer = new Buffer(data.length);
            // Copy the data to the stub buffer
            data.copy(self.stubBuffer, 0);
            // Exit parsing loop
            data = new Buffer(0);
          }
        }
      }
    }
  };
};


/**
 *
 * @constructor
 * @param connection
 */
const AsyncSock = function (connection: any) {
  this.wrapper = connection;
  this.connection = null;


  // Add event listener
  EventEmitter.call(this);

  const self = this;


  // Create new connection instance
  self.connection = self.wrapper.domainSocket
    ? net.createConnection(self.wrapper.host)
    : net.createConnection(self.wrapper.port, self.wrapper.host);

  // Set the options for the connection
  self.connection.setKeepAlive(self.wrapper.keepAlive, self.wrapper.keepAliveInitialDelay);
  self.connection.setTimeout(self.wrapper.connectionTimeout);
  self.connection.setNoDelay(self.wrapper.noDelay);


  self.connection.on('connect', function () {
    // Set socket timeout instead of connection timeout
    self.connection.setTimeout(self.wrapper.socketTimeout);
    // Emit connect event
    self.emit('connect', self.wrapper);
  });


  // Add handlers for events
  self.connection.once('error', errorHandler(self));
  self.connection.once('timeout', timeoutHandler(self));
  self.connection.once('close', closeHandler(self));
  self.connection.on('data', dataHandler(self));

  // copy previous registered callbacks
  const eventTypes = Object.keys(self.wrapper.callbacks);
  eventTypes.forEach(function (key) {
    self.wrapper.callbacks[key].forEach(function (fn: any) {
      self.on(key, fn);
    });
  });
  self.wrapper.callbacks = {};
};

inherits(AsyncSock, EventEmitter);

AsyncSock.prototype.emit = function () {
  const args = Array.prototype.slice.call(arguments);
  const type = args.shift();

  if (args.length > 0 && _.isError(args[0])) {
    args.unshift(type);
  } else {
    args.unshift(type, undefined);
  }

  EventEmitter.prototype.emit.apply(this, args);
};

/* ======================================
 *           Sock getSock
 */

/**
 *
 * @returns {net.Socket}
 */
AsyncSock.prototype.getSock = function () {
  if (this.connection) {
    return this.connection;
  }
  return null;
};


/**
 *
 * @returns {Connection}
 */
AsyncSock.prototype.getConnection = function () {
  return this.wrapper;
};


/* ======================================
 *           Sock destroy
 */


AsyncSock.prototype.destroy = function (fn: any) {
  if (fn) {
    this.once('close', fn);
  }

  if (this.connection) {
    this.connection.end();
    // return this.connection.destroy();
  }

};

/* ======================================
 *           Sock write
 */


AsyncSock.prototype.write = function (buffer: any, fn: any) {
  const self = this;

  if (fn) {
    self.once('data', fn);
  }

  // Write out the command
  // this.connection.write(buffer, 'binary');
  if (!Array.isArray(buffer)) {
    return this.connection.write(buffer, 'binary');
  }
  // Iterate over all buffers and write them in order to the socket
  for (let i = 0; i < buffer.length; i++) {
    this.connection.write(buffer[i], 'binary');
  }
};

/* ======================================
 *           Sock isConnected
 */


AsyncSock.prototype.isConnected = function () {
  // return this.connection.destroyed && this.connection.writable;
  return this.connection != null;
};


/* ======================================
 *           Sock handleError
 */

AsyncSock.prototype.handleError = function (err: any) {
  const self = this;
  if (self.listeners('error').length > 0) {
    self.emit('error', err);
  }
};

/* ======================================
 *           Sock handleTimeout
 */

AsyncSock.prototype.handleTimeout = function () {
  const self = this;
  const conn = self.getConnection();
  self.emit('timeout'
    , f('connection %s to %s:%s timed out', conn.id, conn.host, conn.port)
  );

};

/* ======================================
 *           Sock handleClose
 */

AsyncSock.prototype.handleClose = function (hadError: any) {
  const self = this;
  // var conn = self.getConnection()
  if (!hadError) {
    self.connection = null;
    self.emit('close');
  } else {
    // ???
  }
};

/* ======================================
 *           Sock handleData
 */

AsyncSock.prototype.handleData = function (buffer: any) {
  const self = this;
  const response = self.getConnection().messageHandler(buffer, self);
  self.emit('data', response);
};

/* ======================================
 *           Sock handleData
 */

AsyncSock.prototype.handleParseError = function (errorObject: any) {
  const self = this;
  return self.emit('parseError', errorObject);
};

/* ======================================
 *           Sock handleConnect
 */

AsyncSock.prototype.handleConnect = function () {
};

/**
 * Connect
 * @method
 */
Connection.prototype.connect = function () {
  this.state = 'connect';

  const args = Array.prototype.slice.call(arguments);
  let _options = null;

  if (args.length > 0) {
    if (args.length === 1) {
      // if first argument is an function then it is an async connection
      if (typeof args[0] === 'function') {
        _options = {sync: false};
        this.on('connect', args[0]);
      } else {
        _options = args[0];
      }
    }
  } else {
    if ('connect' in this.callbacks && this.callbacks['connect'].length > 0) {
      _options = {sync: false};
    } else {
      _options = {sync: true};
    }
  }


  const self = this;
  _options = _options || {sync: false};
  // Check if we are overriding the promoteLongs
  if (typeof _options.promoteLongs === 'boolean') {
    self.responseOptions.promoteLongs = _options.promoteLongs;
  }

  self.isSync = false;
  // @ts-ignore
  self.connection = new AsyncSock(self);

  return this;
};


/**
 * Cache callbacks
 * @param eventType
 */
Connection.prototype.on = function (eventType: any, callback: any) {
  if (this.connection) {
    this.connection.on(eventType, callback);
  } else {
    this.callbacks[eventType] = this.callbacks[eventType] || [];
    this.callbacks[eventType].push(callback);
  }

};


/**
 * Destroy connection
 * @method
 */
Connection.prototype.destroy = function (fn: any) {
  if (this.connection) {
    return this.connection.destroy(fn);
  }
};

/**
 * Write to connection
 * @method
 * @param {Command} command Command to write out need to implement toBin and toBinUnified
 */
Connection.prototype.write = function (buffer: any, fn: any) {
  return this.connection.write(buffer, fn);
};


/**
 * Return id of connection as a string
 * @method
 * @return {string}
 */
Connection.prototype.toString = function () {
  return '' + this.id;
};

/**
 * Return json object of connection
 * @method
 * @return {object}
 */
Connection.prototype.toJSON = function () {
  return {id: this.id, host: this.host, port: this.port};
};

/**
 * Is the connection connected
 * @method
 * @return {boolean}
 */
Connection.prototype.isConnected = function () {
  return !this.connection.isConnected();
};


Connection.prototype.destroyAsync = function () {
  return new Promise((resolve, reject) => {
    this.destroy((err: any, res: unknown) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    });
  });
};
Connection.prototype.connectAsync = function () {
  return new Promise((resolve, reject) => {
    this.connect((err: any, res: unknown) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    });
  });
};

Connection.prototype.writeAsync = function (buffer: any) {
  return new Promise((resolve, reject) => {
    this.write(buffer, (err: any, res: unknown) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    });
  });
};
