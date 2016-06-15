const util = require('util');
var inherits = util.inherits
    , f = util.format
    , actions = require('./actions')
    , EventEmitter = require('events').EventEmitter
// TODO load on demand!
    , net = require('net')
    , Sockit = require('./../build/Release/sockit').Sockit
    , Promise = require('bluebird')


/**
 * Implements Socket Connection to Aios Server
 */


var _id = 0;

var Response = actions.Response

function Connection(options) {

    options = options || {}

    // Set empty if no options passed
    this.options = options;
    // Identification information
    this.id = _id++;
    this.debug = options.debug || false


    // Message handler
    this.messageHandler = options.messageHandler ? options.messageHandler : defaultMessageHandler;

    // Max JSON message size
    this.maxBsonMessageSize = options.maxBsonMessageSize || (1024 * 1024 * 16 * 4);


    // Default options
    this.port = options.port || 8118;
    this.host = options.host || 'localhost';
    this.keepAlive = typeof options.keepAlive == 'boolean' ? options.keepAlive : true;
    this.keepAliveInitialDelay = options.keepAliveInitialDelay || 0;
    this.noDelay = typeof options.noDelay == 'boolean' ? options.noDelay : true;
    this.connectionTimeout = options.connectionTimeout || 60000;
    this.socketTimeout = options.socketTimeout || 60000;

    // Check if we have a domain socket
    this.domainSocket = this.host.indexOf('\/') != -1;

    // Internal state
    this.connection = null;
    this.isSync = false;
    this.callbacks = {};
}


var defaultMessageHandler = function (buffer) {
    var response = new Response(buffer);
    response.parse()
    return response;
}


//
// Connection handlers
var errorHandler = function (sock) {
    return function (err) {
        // Debug information
        var conn = sock.getConnection();
        if(conn.debug) {
            console.trace(f('connection %s for [%s:%s] errored out with [%s]', conn.id, conn.host, conn.port, JSON.stringify(err)));
        }
        // Emit the error
        sock.handleError(err);

    }
}

var timeoutHandler = function (self) {
    return function () {
        // Debug information
        var conn = self.getConnection();
        if(conn.debug){
            console.trace(f('connection %s for [%s:%s] timed out', conn.id, conn.host, conn.port));
        }
        // Emit timeout error
        self.handleTimeout()
    }
}

var closeHandler = function (self) {
    return function (hadError) {
        // Debug information
        var conn = self.getConnection();
        if(conn.debug) {
            console.trace(f('connection %s with for [%s:%s] closed', conn.id, conn.host, conn.port));
        }
        // Emit close event
        self.handleClose(hadError)
    }
}

var dataHandler = function (self) {
    return function (data) {
        var maxBsonMessageSize = self.wrapper.maxBsonMessageSize
        // Parse until we are done with the data
        while (data.length > 0) {
            // If we still have bytes to read on the current message
            if (self.bytesRead > 0 && self.sizeOfMessage > 0) {
                // Calculate the amount of remaining bytes
                var remainingBytesToRead = self.sizeOfMessage - self.bytesRead;
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
                        var emitBuffer = self.buffer;
                        // Reset state of buffer
                        self.buffer = null;
                        self.sizeOfMessage = 0;

                        self.bytesRead = 0;
                        self.stubBuffer = null;
                        // Emit the buffer
                        self.handleData(emitBuffer);

                    } catch (err) {
                        var errorObject = {
                            err: "socketHandler",
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
                        var newData = new Buffer(self.stubBuffer.length + data.length);
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
                        var newStubBuffer = new Buffer(self.stubBuffer.length + data.length);
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
                        //var totalLength = data[0] | data[1] << 8 | data[2] << 16 | data[3] << 24;
                        //var totalLength = data[0]  | data[1] << 8 | data[2] << 16 | data[3] << 24;
                        var totalLength = Utils.encodeTotalLength(data);

                        var sizeOfMessage = totalLength + 8;
                        // If we have a negative totalLength emit error and return
                        if (sizeOfMessage < 0 || sizeOfMessage > maxBsonMessageSize) {
                            var errorObject = {
                                err: "socketHandler",
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

                        } else if (sizeOfMessage > 8 && sizeOfMessage < maxBsonMessageSize && sizeOfMessage == data.length) {
                            try {
                                var emitBuffer = data;
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
                                var errorObject = {
                                    err: "socketHandler",
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
                            var errorObject = {
                                err: "socketHandler",
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
                            var emitBuffer = data.slice(0, sizeOfMessage);
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
                        self.stubBuffer = new Buffer(data.length)
                        // Copy the data to the stub buffer
                        data.copy(self.stubBuffer, 0);
                        // Exit parsing loop
                        data = new Buffer(0);
                    }
                }
            }
        }
    }
}



/**
 *
 * @param [Connection] connection
 * @constructor
 */
var SyncSock = function (connection) {
    this.wrapper = connection
    this.connection = null
    var self = this;

    self.connection = new Sockit();
    self.connection.setPollTimeout(self.wrapper.socketTimeout);
    self.connection.connect({host: self.wrapper.host, port: self.wrapper.port});

}

SyncSock.prototype.on = function (eventType, callback) {
    throw new Error("Sync sock mode, we don't fire events!");
}



/**
 *
 * @param [Connection] connection
 * @constructor
 */
var AsyncSock = function (connection) {
    this.wrapper = connection
    this.connection = null


    // Add event listener
    EventEmitter.call(this);

    var self = this;


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
    var eventTypes = Object.keys(self.wrapper.callbacks);
    eventTypes.forEach(function (key) {
        self.wrapper.callbacks[key].forEach(function (fn) {
            self.on(key, fn);
        })
    })
    self.wrapper.callbacks = {}
}

inherits(AsyncSock, EventEmitter);

AsyncSock.prototype.emit = function(){
    var args = Array.prototype.slice.call(arguments);
    var type = args.shift()

    if(args.length > 0 && util.isError(args[0])){
        args.unshift(type)
    }else{
        args.unshift(type, undefined)
    }

    EventEmitter.prototype.emit.apply(this,args)
}

/* ======================================
 *           Sock getSock
 */

/**
 *
 * @returns {netSync.SockIt}
 */
SyncSock.prototype.getSock = function () {
    if (this.connection) return this.connection;
    return null;
}

/**
 *
 * @returns {net.Socket}
 */
AsyncSock.prototype.getSock = function () {
    if (this.connection) return this.connection;
    return null;
}

/**
 *
 * @returns {Connection}
 */
SyncSock.prototype.getConnection = function () {
    return this.wrapper;
}

/**
 *
 * @returns {Connection}
 */
AsyncSock.prototype.getConnection = function () {
    return this.wrapper;
}


/* ======================================
 *           Sock destroy
 */

SyncSock.prototype.destroy = function (fn) {
    if (this.connection) {
        this.connection.close();
    }
    this.connection = null
}


AsyncSock.prototype.destroy = function (fn) {
    if(fn){
        this.once('close',fn)
    }

    if (this.connection) {
        this.connection.end();
        //return this.connection.destroy();
    }

}

/* ======================================
 *           Sock write
 */

SyncSock.convertToBuffer = function(data){

        var buffer = new Buffer(data.length);
        data.copy(buffer);
        return buffer;
}


SyncSock.prototype.write = function (buffer, skipread) {
    if (!Array.isArray(buffer)){	
        var _buffer = SyncSock.convertToBuffer(buffer);
        this.connection.write(_buffer);
    }else{
        for (var i = 0; i < buffer.length; i++) {
            var _buffer = SyncSock.convertToBuffer(buffer[i]);
            this.connection.write(_buffer);
        }
    }


    skipread = skipread || false
    if (!skipread) {

        var resLength = this.connection.read(4);
        var totalLength = Utils.encodeTotalLength(resLength);
        var buffer = new Buffer(totalLength + 8);
        resLength.copy(buffer);
        var content = this.connection.read(totalLength + 4);
        content.copy(buffer,4);

        return this.handleData(buffer);
    }
}


AsyncSock.prototype.write = function (buffer, fn) {
    var self = this

    if(fn){
        self.once('data',fn)
    }

    // Write out the command
    // this.connection.write(buffer, 'binary');
    if (!Array.isArray(buffer)) {
        return this.connection.write(buffer, 'binary');
    }
    // Iterate over all buffers and write them in order to the socket
    for (var i = 0; i < buffer.length; i++) {
        this.connection.write(buffer[i], 'binary');
    }
}

/* ======================================
 *           Sock isConnected
 */

SyncSock.prototype.isConnected = function () {
    return this.connection != null;
}


AsyncSock.prototype.isConnected = function () {
    // return this.connection.destroyed && this.connection.writable;
    return this.connection != null;
}


/* ======================================
 *           Sock handleError
 */
SyncSock.prototype.handleError = function () {
}

AsyncSock.prototype.handleError = function (err) {
    var self = this
    if (self.listeners('error').length > 0){
        self.emit("error", err);
    }
}

/* ======================================
 *           Sock handleTimeout
 */
SyncSock.prototype.handleTimeout = function () {
}

AsyncSock.prototype.handleTimeout = function () {
    var self = this
    var conn = self.getConnection()
    self.emit("timeout"
        , f("connection %s to %s:%s timed out", conn.id, conn.host, conn.port)
        );

}

/* ======================================
 *           Sock handleClose
 */
SyncSock.prototype.handleClose = function () {
    var self = this
    self.connection = null
}

AsyncSock.prototype.handleClose = function (hadError) {
    var self = this
    // var conn = self.getConnection()
    if (!hadError) {
        self.connection = null
        self.emit("close");
    }else{
        // ???
    }
}

/* ======================================
 *           Sock handleData
 */
SyncSock.prototype.handleData = function (buffer) {
    var self = this
    return self.getConnection().messageHandler(buffer, self);
}

AsyncSock.prototype.handleData = function (buffer) {
    var self = this
    var response = self.getConnection().messageHandler(buffer, self);
    self.emit("data", response);
}

/* ======================================
 *           Sock handleData
 */
SyncSock.prototype.handleParseError = function () {
}

AsyncSock.prototype.handleParseError = function (errorObject) {
    var self = this
    return self.emit("parseError", errorObject);
}

/* ======================================
 *           Sock handleConnect
 */
SyncSock.prototype.handleConnect = function () {
}

AsyncSock.prototype.handleConnect = function () {
}

/**
 * Connect
 * @method
 */
Connection.prototype.connect = function () {
    this.state = 'connect'

    var args = Array.prototype.slice.call(arguments);
    var _options = null;

    if (args.length > 0) {
        if (args.length == 1) {
            // if first argument is an function then it is an async connection
            if (typeof args[0] == 'function') {
                _options = {sync: false}
                this.on('connect', args[0]);
            } else {
                _options = args[0];
            }
        }
    } else {
        if ('connect' in this.callbacks && this.callbacks['connect'].length > 0) {
            _options = {sync: false}
        } else {
            _options = {sync: true}
        }
    }


    var self = this;
    _options = _options || {sync: false};
    // Check if we are overriding the promoteLongs
    if (typeof _options.promoteLongs == 'boolean') {
        self.responseOptions.promoteLongs = _options.promoteLongs;
    }

    self.isSync = _options.sync ? _options.sync : false
    self.connection = self.isSync ? new SyncSock(self) : new AsyncSock(self);

    return this;
}



/**
 * Cache callbacks
 * @param eventType
 */
Connection.prototype.on = function (eventType, callback) {
    if (this.connection) {
        this.connection.on(eventType, callback)
    } else {
        this.callbacks[eventType] = this.callbacks[eventType] || []
        this.callbacks[eventType].push(callback);
    }

}


/**
 * Destroy connection
 * @method
 */
Connection.prototype.destroy = function (fn) {
    if (this.connection) return this.connection.destroy(fn);
}

/**
 * Write to connection
 * @method
 * @param {Command} command Command to write out need to implement toBin and toBinUnified
 */
Connection.prototype.write = function (buffer, fn) {
    return this.connection.write(buffer, fn);
}


/**
 * Return id of connection as a string
 * @method
 * @return {string}
 */
Connection.prototype.toString = function () {
    return "" + this.id;
}

/**
 * Return json object of connection
 * @method
 * @return {object}
 */
Connection.prototype.toJSON = function () {
    return {id: this.id, host: this.host, port: this.port};
}

/**
 * Is the connection connected
 * @method
 * @return {boolean}
 */
Connection.prototype.isConnected = function () {
    return !this.connection.isConnected()
}


var Utils = function () {
}

Utils.encodeTotalLength = function (data) {
    return data[3] | data[2] << 8 | data[1] << 16 | data[0] << 24;
}


Connection.prototype.destroyAsync = Promise.promisify(Connection.prototype.destroy)
Connection.prototype.connectAsync = Promise.promisify(Connection.prototype.connect)
Connection.prototype.writeAsync = Promise.promisify(Connection.prototype.write)


module.exports = Connection
