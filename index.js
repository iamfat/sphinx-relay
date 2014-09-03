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
config.relay = config.relay || {};

var ZeroMQ = require('zmq');
var mqPath = config.relay.mq || 'ipc:///var/run/sphinx-relay.mq';
var mq = ZeroMQ.socket('push');
mq.bind(mqPath, function(err) {
    if (err) {
        Winston.error(err.message);
        throw err;
    }
});

// run several workers
var num = config.relay.workers || 3;
var forever = require('forever-monitor');
while (num--) {
    var workerOpt = {};
    if (argv.v) workerOpt.options = ['-v'];
    new (forever.Monitor)('worker.js', workerOpt).start();
}

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
        
        if (/^(INSERT|REPLACE|UPDATE)/i.test(SQL)) {

            Winston.debug("RELAY[" + process.pid + "] #" + _id + " query: " + SQL);
        
            // return OK immediately...
            conn.writeOk({
                fieldCount: 0,
                affectedRows: 0,
                insertId: 1, // insertId cannot be 0, just set it to 1. :(
                serverStatus: 2,
                warningStatus: 0
            });
            
            // push command to message queue...
            mq.send(SQL);
            
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

server.listen(config.relay.port || 9306);
