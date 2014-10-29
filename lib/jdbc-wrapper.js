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
                                        //__dirname + '/../build/lib/DBPool-5.1.jar',
                                        __dirname + '/../build/lib/tomcat-jdbc.jar',
                                        __dirname + '/../build/lib/tomcat-juli.jar',
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
                                ';DBSERVERNAME=' +  self._config.database + ';';
        

            connectionPool[self._config.database] = java.newInstanceSync("org.apache.tomcat.jdbc.pool.DataSource");

            connectionPool[self._config.database].setDriverClassNameSync("com.informix.jdbc.IfxDriver");
            connectionPool[self._config.database].setUrlSync(url);
            connectionPool[self._config.database].setUsernameSync(self._config.user);
            connectionPool[self._config.database].setPasswordSync(self._config.password);
            connectionPool[self._config.database].setInitialSizeSync(10);
            connectionPool[self._config.database].setMaxActiveSync(100);
            connectionPool[self._config.database].setMaxIdleSync(50);
            connectionPool[self._config.database].setMinIdleSync(10);
            connectionPool[self._config.database].setTestOnBorrowSync(true);
            connectionPool[self._config.database].setTestWhileIdleSync(true);
            connectionPool[self._config.database].setValidationQuerySync("SELECT 1");
            connectionPool[self._config.database].setValidationIntervalSync(30000);
            connectionPool[self._config.database].setMaxAgeSync(1800000);

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
    if (!connectionPool[self._config.database])  {
        if (callback) callback("Connection pool not initialized");
        return;
    }
    connectionPool[self._config.database].getConnection(function(err, conn) {
        if (err) {
            connectionPool[self._config.database] = null;
            self.emit('error', err || 'Connection null');
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
        self.emit('finish', rows);

        if (callback) {
            if (err) {
                callback(err, null);
            } else {
                callback(null, JSON.parse(rows));
            }
        }
    });
};

JDBCConn.prototype.executePreparedQuery = function(sql, callback, params) {
    var self = this;

    self.emit('start', sql);

    self._informixWrapper.executePreparedQuery(sql, JSON.stringify(params), function(err, rows) {
        self.emit('finish', rows);

        if (callback) {
            if(err) {
                callback(err, null);
            } else {
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
        self.emit('finish', count);

        if(callback) {
            if(err) {
                callback(err, null);
            } else {
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
        self.emit('finish', count);

        if (callback) {
            if (err) {
                callback(err, null);
            } else {
                callback(null, count);
            }
        }
    });

};

module.exports = JDBCConn;
