var MySQL = require('mysql2');
var YAML = require('js-yaml');

var argv = require('optimist').argv;

var Winston = require('winston');
Winston.remove(Winston.transports.Console).add(Winston.transports.Console, {
  level: argv.v ? 'debug' : 'info',
  colorize: true,
  silent: false,
  timestamp: true
});

var config = YAML.safeLoad(require('fs').readFileSync('./config/default.yaml', 'utf8'));
var server = MySQL.createServer();

var _id=0;
server.on('connection', function(conn) {

    Winston.info("#" + _id + " connected.");
    
    conn.serverHandshake({
        protocolVersion: 10,
        serverVersion: 'sphinx-relay',
        connectionId: _id++,
        statusFlags: 2,
        characterSet: 8,
        capabilityFlags: 0xffffff
    });

    conn.on('field_list', function(table, fields) {
        Winston.debug("#" + _id + ' field list:');
        Winston.debug(table);
        Winston.debug(fields);
        conn.writeEof();
    });

    var opt = {};
    
    if (config.socketPath) {
        opt.socketPath = config.socketPath;
    } else {
        opt.host = config.remote.host || 'localhost';
        opt.port = config.remote.port || 9306;
    }
    
    // use for test with mysql server...
    //===========
    if (config.remote.user) {
        opt.user = config.remote.user;
    }

    if (config.remote.password) {
        opt.password = config.remote.password;
    }

    if (config.remote.database) {
        opt.database = config.remote.database;
    }
    //===========

    var remote = MySQL.createConnection(opt);
    
    remote.on('error', function(err) {
        conn.writeError({ code: 1064, message: err.message});
    });

    conn.on('query', function(SQL) {
        
        Winston.debug("#" + _id + " query: " + SQL);
        
        if (/^(INSERT|REPLACE|UPDATE)/i.test(SQL)) {

            // return OK immediately...
            conn.writeOk({
                fieldCount: 0,
                affectedRows: 0,
                insertId: 9999999, // insertId cannot be 0, just set it to a big number :(
                serverStatus: 2,
                warningStatus: 0 
            });
            
            // TODO: push command to message queue...

            return;
        }

        remote.query(SQL, function(err) { // overloaded args, either (err, result :object)
            // or (err, rows :array, columns :array)
            if (err) {
                conn.writeError({ code: 1064, message: err.message});
                return;
            }
            
            if (Array.isArray(arguments[1])) {
                // response to a 'select', 'show' or similar
                var rows = arguments[1], columns = arguments[2];
                conn.writeTextResult(rows, columns);
            } else {
                // response to an 'insert', 'update' or 'delete'
                var result = arguments[1];
                console.log(result);
                conn.writeOk(result);
            }
        });
        
    });

    conn.on('error', function(err) {
        // Winston.error(err.message);
    });

    conn.on('end', remote.end.bind(remote));    
});

config.relay = config.relay || {};

server.listen(config.relay.port || 9306);
