/*
 * Copyright (C) 2013 TopCoder Inc., All Rights Reserved.
 *
 * @version 1.1
 * @author pvmagacho
 * changes in 1.1
 * - increase the max listeners when execute the query.
 */
"use strict";
/*jslint stupid: true */

var EventEmitter = require('events').EventEmitter;
var java = require('java');
var sys = require('sys');
var _ = require('underscore');

var connectionPool = {};

// Initialize java environment
var defaultLog = function (msg) {
    console.log(new Date() + " - " + msg);
};

// Class path configuration - Informix driver, JDBC Connection pool and
// custom InformixWrapper class.
java.classpath = java.classpath.concat([__dirname + '/../build/lib/ifxjdbc.jar',
                                        __dirname + '/../build/lib/commons-logging-1.1.3.jar',
                                        __dirname + '/../build/lib/DBPool-5.1.jar',
                                        __dirname + '/../build/lib/gson-2.2.4.jar',
                                        __dirname + '/../build/lib/informix-wrapper.jar']);

// Leave signal handling to actionHero. JVM will ignore system singals.
// See http://docs.oracle.com/javase/6/docs/technotes/tools/solaris/java.html
// for more information.
java.options.push('-Xrs');

// Register informix driver
java.newInstance('com.informix.jdbc.IfxDriver', function(err, driver) {
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
}

sys.inherits(JDBCConn, EventEmitter);

JDBCConn.prototype.initialize = function() {
    var self = this;
    try {
        if (!connectionPool[self._config.database]) {
            var url = 'jdbc:informix-sqli://' + self._config.host + ':' + self._config.port + '/' +
                                self._config.database + ':INFORMIXSERVER=' + self._config.server +
                                ';INFORMIXCONRETRY=1;INFORMIXCONTIME=' + (self._config.timeout/1000) + ';';

            var props = java.newInstanceSync("java.util.Properties");
            props.setPropertySync("user", self._config.user);
            props.setPropertySync("password", self._config.password);
            //props.setPropertySync("validator", "snaq.db.Select1Validator");

            var validator = java.newInstanceSync("snaq.db.SimpleQueryValidator", "select 1 from dual");


            connectionPool[self._config.database] = java.newInstanceSync("snaq.db.ConnectionPool",   // class name
                                                  "pool-" + self._config.database,                   // poolname
                                                  self._config.minpool,                              // minpool
                                                  self._config.maxpool,                              // maxpool
                                                  self._config.maxsize,                              // maxsize
                                                  self._config.idleTimeout,                          // idleTimeout
                                                  url,
                                                  props);                                               // url
                                                 // self._config.user,                                 // username
                                                  //self._config.password);                 // password
            connectionPool[self._config.database].setValidatorSync(validator);

            connectionPool[self._config.database].registerShutdownHookSync();

            // console.trace('Initializing for url ' + url);
            self._log('Initialized pool for url - ' + url, 'info');
        }
    } catch (ex) {
        connectionPool[self._config.database] = null;
        self.emit('error', ex);
        self._log('Exception: ' + ex);
    }

    return self;
};

JDBCConn.prototype.connect = function(callback) {
    var self = this;
    // console.log('Connecting ' + self._config.database);
    if (!connectionPool[self._config.database])  {
        if (callback) callback("Connection pool not initialized");
        return;
    }
    connectionPool[self._config.database].getConnection(self._config.timeout, function(err, conn) {
        if (err) {
            connectionPool[self._config.database] = null;
            self.emit('error', err || 'Connection null');
            if (callback) callback(err);
        } else {
            self._conn = conn;
            java.newInstance('com.topcoder.node.jdbc.InformixWrapper', conn, function(err, wrapper) {
                self._informixWrapper = wrapper;
                if (callback) callback(err);
            });
        }
    });
};

JDBCConn.prototype.disconnect = function() {
    var self = this;
    // console.log('Disconnecting ' + self._config.database);
    if (self.isConnected()) {
        self._conn.close(function() {
            self.emit('close', null);
            // console.log('Closed the database connection');
        });
    }
};

JDBCConn.prototype.isConnected = function() {
    var self = this;
    return (self._conn != null && !self._conn.isClosedSync());
};

JDBCConn.prototype.beginTransaction = function(callback) {
    this._conn.setAutoCommit(false, callback);
};

JDBCConn.prototype.endTransaction = function(error, callback) {
    if (error) {
        this._conn.rollback(callback);
    } else {
        this._conn.commit(callback);
    }
};

JDBCConn.prototype.query = function(sql, callback, config) {
    var self = this;

    if (typeof callback !== "function") {
        self.emit('error', "Callback must be a function");
        return null;
    }

    self.setMaxListeners(15);

    self._sql = sql.trim();
    self._callback = callback;

    for (var key in config) {
        if (config.hasOwnProperty(key) && _.isFunction(config[key]) ) {
            self.once(key, config[key]);
        }
    }

    return self;
};

JDBCConn.prototype.execute = function(params) {
    var self = this;
    var callback = self._callback;

    if (self._sql.toLowerCase().indexOf('insert') === 0 ||
        self._sql.toLowerCase().indexOf('update') === 0 ||
        self._sql.toLowerCase().indexOf('delete') === 0 ||
        self._sql.toLowerCase().indexOf('create') === 0) {
        if (params !== null && undefined !== params) {
            self.executePreparedUpdate(self._sql, self._callback, params);
        } else {
            self.executeUpdate(self._sql, self._callback);
        }
    } else {
        if (params !== null && undefined !== params) {
            self.executePreparedQuery(self._sql, self._callback, params);
        } else {
            self.executeQuery(self._sql, self._callback);
        }
    }
};

JDBCConn.prototype.executeQuery = function(sql, callback) {
    var self = this;

    self.emit('start', sql);

    self._informixWrapper.executeQuery(sql, function(err, rows) {
        var random = Math.random();
        // console.log('executeQuery - start   ' + random);
        self.emit('finish', rows);

        if (callback) {
            if (err) {
                console.trace('executeQuery - err ' + random);
                callback(err, null);
            } else {
                // console.log('executeQuery - success ' + random);
                callback(null, JSON.parse(rows));
            }
        }
    });
};

JDBCConn.prototype.executePreparedQuery = function(sql, callback, params) {
    var self = this;

    self.emit('start', sql);

    self._informixWrapper.executePreparedQuery(sql, JSON.stringify(params), function(err, rows) {
        var random = Math.random();
        // console.log('executePreparedQuery - start   ' + random);
        self.emit('finish', rows);

        if (callback) {
            if(err) {
                console.trace('executePreparedQuery - err ' + random);
                callback(err, null);
            } else {
                // console.log('executePreparedQuery - success ' + random);
                callback(null, JSON.parse(rows));
            }
        }
    });
};

JDBCConn.prototype.executeUpdate = function(sql, callback) {
    var self = this;

    self.emit('start', sql);

    // these are asynchronous calls

    self._informixWrapper.executeUpdate(sql, function(err, count) {
        var random = Math.random();
        // console.log('executeUpdate - start   ' + random);
        self.emit('finish', count);

        if(callback) {
            if(err) {
                console.trace('executeUpdate - err ' + random);
                callback(err, null);
            } else {
                // console.log('executeUpdate - success ' + random);
                callback(null, count);
            }
        }
    });
};

JDBCConn.prototype.executePreparedUpdate = function(sql, callback, params) {
    var self = this;

    self.emit('start', sql);

    // these are asynchronous calls
    self._informixWrapper.executePreparedUpdate(sql, JSON.stringify(params), function(err, count) {
        var random = Math.random();
        // console.log('executePreparedUpdate - start   ' + random);
        self.emit('finish', count);

        if (callback) {
            if (err) {
                console.trace('executePreparedUpdate - err     ' + random);
                callback(err, null);
            } else {
                // console.log('executePreparedUpdate - success ' + random);
                callback(null, count);
            }
        }
    });

};

module.exports = JDBCConn;
