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

var worker = ZeroMQ.socket('pull');
worker.connect(mqPath);
Winston.info('#'+process.pid+' worker connected!');

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
    Winston.error(err.message);
    setTimeout(function(){
        process.exit(1);
    }, 1000);
});

worker.on('message', function(SQL) {
    Winston.debug('WORKER[' + process.pid + ']' + ': ' + SQL);
    if (!SQL) return;
    
    remote.query(SQL.toString('utf8'), function(err) { // overloaded args, either (err, result :object)
        // or (err, rows :array, columns :array)
        if (err) {
            Winston.error(err.message);
            return;
        }
        
        // We actually don't care about the result...
        if (Array.isArray(arguments[1])) {
            // response to a 'select', 'show' or similar
            var rows = arguments[1], columns = arguments[2];
            Winston.debug({rows:rows, columns:columns});
        } else {
            // response to an 'insert', 'update' or 'delete'
            var result = arguments[1];
            Winston.debug(result);
        }
    });
    
});

