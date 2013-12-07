/*
 * Copyright (C) 2013 TopCoder Inc., All Rights Reserved.
 *
 * @version 1.0
 * @author pvmagacho
 *
 * original from https://github.com/CraZySacX/node-jdbc
 */
"use strict";
/*jslint stupid: true */

var EventEmitter = require('events').EventEmitter;
var java = require('java');
var sys = require('sys');
var _ = require('underscore');

var DEFAULT_TIMEOUT = 30000; // 30s

var connectionPool = {};

// Initialize java environment
var defaultLog = function (msg) {
    console.log(new Date() + " - " + msg);
};

java.classpath = java.classpath.concat([__dirname + '/../build/lib/ifxjdbc.jar',
                                        __dirname + '/../build/lib/commons-logging-1.1.3.jar',
                                        __dirname + '/../build/lib/DBPool-5.1.jar',
                                        __dirname + '/../build/lib/gson-2.2.4.jar',
                                        __dirname + '/../build/lib/informix-wrapper.jar']);
// Leave signal handling to actionHero
java.options.push('-Xrs');
java.newInstance('com.informix.jdbc.IfxDriver', function(err, driver) {
	console.log(new Date() + " - Initializing informix driver");
	java.callStaticMethodSync('java.sql.DriverManager', 'registerDriver', driver);
});

/**
 * Constructor
 */
var JDBCConn = function (config, log) {
    EventEmitter.call(this);
    this._conn = null;
    this._config = config;

    this._log = log || defaultLog;

    if (!connectionPool[config.database]) {
        this.initialize(config);
    }
}

sys.inherits(JDBCConn, EventEmitter);

JDBCConn.prototype.initialize = function(config, callback) {
    var self = this;
    self._config = config;

    try {
        var url = 'jdbc:informix-sqli://' + self._config.host + ':' + self._config.port + '/' +
                            self._config.database + ':INFORMIXSERVER=' + self._config.server + ';';

        connectionPool[self._config.database] = java.newInstanceSync("snaq.db.ConnectionPool",   // class name
                                              "pool-" + self._config.database,                   // poolname
                                              1,                                                 // minpool
                                              10,                                                // maxpool
                                              0,                                                 // maxsize
                                              self._config.idleTimeout,							 // idleTimeout
                                              url,                                               // url
                                              self._config.user,                                 // username
                                              self._config.password);                            // password
        connectionPool[self._config.database].registerShutdownHookSync();

        self._log('Initialized pool for url - ' + url, 'info');
        if (callback) callback(null);
    } catch (ex) {
        self.emit('error', ex);
        self._log('Exception: ' + ex);
        if (callback) callback(ex);
    }
};

JDBCConn.prototype.connect = function(callback) {
    var self = this;
    if (!connectionPool[self._config.database])  {
        if (callback) callback("Connection pool not initialized");
        return;
    }
    connectionPool[self._config.database].getConnection(DEFAULT_TIMEOUT, function(err, conn) {
        if (err) {
            self.emit('error', err || 'Connection null');
        } else {
            self._conn = conn;
            self._handleResult = java.newInstanceSync('com.topcoder.node.jdbc.HandleResultSet', conn);

            if (callback) callback(null);
        }
    });
};

JDBCConn.prototype.disconnect = function() {
    var self = this;
    if (self.isConnected()) {
        self._conn.close(function() {
            self.emit('close', null);
        });
    }
};

JDBCConn.prototype.isConnected = function() {
    var self = this;
    return (self._conn != null && !self._conn.isClosedSync());
};

JDBCConn.prototype.query = function(sql, callback, config) {
    var self = this;

    self._sql = sql;
    self._callback = callback;

    for (var key in config) {
        if (config.hasOwnProperty(key) && _.isFunction(config[key]) ) {
            self.on(key, config[key]);
        }
    }

    return self;
};

JDBCConn.prototype.execute = function() {
    var self = this;
    var callback = self._callback;

    if (self._sql.toLowerCase().indexOf('insert') === 0 ||
        self._sql.toLowerCase().indexOf('update') === 0 ||
        self._sql.toLowerCase().indexOf('delete') === 0) {
        self.executeUpdate(self._sql, self._callback);
    } else {
        self.executeQuery(self._sql, self._callback);
    }
};

JDBCConn.prototype.executeQuery = function(sql, callback) {
    var self = this;

    self.emit('start', sql);

    try {
    	self._handleResult.executeQuery(sql, function(err, rows) {
    		self._log('Rows: ' + rows, 'debug');
            if (callback) callback(null, JSON.parse(rows));
    	});
    } catch(e) {
        if (callback) callback(e, null);
    }
};

JDBCConn.prototype.executeUpdate = function(sql, callback) {
    var self = this;

    self.emit('start', sql);

    // these are asynchronous calls
    try {
        self._handleResult.executeUptate(sql, function(err, count) {
    		self._log('Count: ' + count, 'debug');
            if (callback) callback(null, count);
    	});
    } catch (e) {
        if (callback) callback(e, null);
    }
};

module.exports = JDBCConn;
