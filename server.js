var P = require("bluebird");
var moment = require('moment');

var express    = require('express');
var app        = express();
var server = require('http').Server(app);
var io = require('socket.io')(server).of('/api');
var bodyParser = require('body-parser');

var identifi = require('identifi-lib');
var Message = identifi.message;
var identifiClient = identifi.client;
var pkg = require('./package.json');

var os = require('os');
var fs = require('fs');
var util = require('util');

var keyutil = require('identifi-lib/keyutil');
var datadir = process.env.IDENTIFI_DATADIR || (os.homedir() + '/.identifi');
var myKey = keyutil.getDefault(datadir);

process.env.NODE_CONFIG_DIR = __dirname + '/config';
var config = require('config');

var outgoingConnections = {};

if (process.env.NODE_ENV !== 'test') {
  // Extend default config from datadir/config.json and write the result back to it
  (function setConfig() {
    if (!fs.existsSync(datadir)) {
      fs.mkdirSync(datadir);
    }
    var cfgFile = datadir + '/config.json';
    if (fs.existsSync(cfgFile)) {
      var cfgFromFile = require(cfgFile);
      Object.assign(config, cfgFromFile);
    }
    fs.writeFileSync(cfgFile, JSON.stringify(config, null, 4), 'utf8');
    // Set some paths
    if (config.db.connection.filename) {
      config.db.connection.filename = datadir + '/' + config.db.connection.filename;
    }
    config.logfile = datadir + '/' + config.logfile;
  })();
}

var logStream = fs.createWriteStream(config.get('logfile'), {flags: 'a', encoding: 'utf8'});

function log(msg) {
  msg = moment.utc().format() + ": " + util.format(msg);
  logStream.write(msg + '\n');
  console.log(msg);
}

process.on("uncaughtException", function(e) {
  log(e);
});

// Init DB
var knex, db;
try {
  var dbConf = config.get('db');
  knex = require('knex')(dbConf);
  db = require('./db.js')(knex);
  db.init.return();
} catch (ex) {
  log(ex);
  process.exit(0);
}

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

var port = config.get('port');

// Routes
// =============================================================================
var router = express.Router();


function handleError(err, req, res) {
  log(err);
  log(req);
  res.status(500).json('Server error');
}


function emitMsg(msg) {
  io.emit('msg', { jws: msg.jws, hash: msg.hash });
  Object.keys(outgoingConnections).forEach(function(key) {
    outgoingConnections[key].emit('msg', { jws: msg.jws, hash: msg.hash });
  });
}



router.get('/', function(req, res) {
  var queries = [db.getMessageCount()];
  P.all(queries).then(function(results) {
    res.json({ message: 'Identifi API',
                version: pkg.version,
                msgCount: results[0][0].val,
                publicKey: myKey.public.hex
              });
  }).catch(function(err) { handleError(err, req, res); });
});



router.route('/peers')
  .get(function(req, res) {
    db.getPeers()
    .then(function(dbRes) {
      res.json(dbRes);
    }).catch(function(err) { handleError(err, req, res); });
  })

  .post(function(req, res) {
    res.json("add peer");
  })

  .delete(function(req, res) {
    res.json("remove peer");
  });


// Helper method
function getMessages(req, res, options) {
    options = options || {};
    options.where = options.where || {};

    if (req.query.viewpoint_type && req.query.viewpoint_value) {
      options.viewpoint = [req.query.viewpoint_type, req.query.viewpoint_value];
    }
    if (req.query.max_distance) { options.maxDistance = parseInt(req.query.max_distance); }
    if (req.query.type)     { options.where['Messages.type'] = req.query.type; }
    if (req.query.order_by) { options.orderBy = req.query.order_by; }
    if (req.query.direction && (req.query.direction === 'asc' || req.query.direction === 'desc')) {
       options.direction = req.query.order_by;
    }
    if (req.query.limit)    { options.limit = parseInt(req.query.limit); }
    if (req.query.offset)   { options.offset = parseInt(req.query.offset); }
    db.getMessages(options).then(function(dbRes) {
      res.json(dbRes);
    }).catch(function(err) { handleError(err, req, res); });
}



router.route('/messages')
  .get(function(req, res) {
    getMessages(req, res);
  })

  .post(function(req, res) {
    var m = req.body;

    db.messageExists(m.hash)
    .then(function(exists) {
      if (!exists) {
        Message.verify(m);
        db.saveMessage(m);
        emitMsg(m);
        res.status(201).json(m);
      } else {
        res.status(200).json(m);
      }
    }).catch(function(err) { handleError(err, req, res); });
  });



router.route('/messages/:hash')
  .get(function(req, res) {
    db.getMessages({ where: { hash: req.params.hash } }).then(function(dbRes) {
      if (!dbRes.length) {
        return res.status(404).json('Message not found');
      }
      res.json(dbRes[0]);
    }).catch(function(err) { handleError(err, req, res); });
  })

  // TODO: permissions...
  .delete(function(req, res) {
    db.dropMessage(req.params.hash).then(function(dbRes) {
      if (!dbRes) {
        return res.status(404).json('Message not found');
      }
      res.json('OK');
    }).catch(function(err) { handleError(err, req, res); });
  });



router.get('/id', function(req, res) {
    var options = {
      where: {}
    };
    if (req.query.type)             { options.where.type = req.query.type; }
    if (req.query.search_value)     { options.searchValue = req.query.search_value; }
    if (req.query.order_by)         { options.orderBy = req.query.order_by; }
    if (req.query.direction && (req.query.direction === 'asc' || req.query.direction === 'desc'))
                                    { options.direction = req.query.order_by; }
    if (req.query.limit)            { options.limit = parseInt(req.query.limit); }
    if (req.query.offset)           { options.offset = parseInt(req.query.offset); }
    db.getIdentities(options).then(function(dbRes) {
      res.json(dbRes);
    }).catch(function(err) { handleError(err, req, res); });
});



router.get('/id/:type/:value', function(req, res) {
  db.getIdentities({ where: { type: req.params.type, value: req.params.value } }).then(function(dbRes) {
    res.json(dbRes);
  }).catch(function(err) { handleError(err, req, res); });
});



router.get('/id/:id_type/:id_value/stats', function(req, res) {
  var options = {};

  if (req.query.viewpoint_type && req.query.viewpoint_value) {
    options.viewpoint = [req.query.viewpoint_type, req.query.viewpoint_value];
  }
  if (req.query.max_distance) { options.maxDistance = parseInt(req.query.max_distance); }

  db.getStats([req.params.id_type, req.params.id_value], options).then(function(dbRes) {
    res.json(dbRes);
  }).catch(function(err) { handleError(err, req, res); });
});


router.get('/id/:id_type/:id_value/sent', function(req, res) {
  var options = {
    author: [req.params.id_type, req.params.id_value],
  };
  getMessages(req, res, options);
});


router.get('/id/:id_type/:id_value/received', function(req, res) {
  var options = {
    recipient: [req.params.id_type, req.params.id_value],
  };
  getMessages(req, res, options);
});



router.get('/id/:id_type/:id_value/connections', function(req, res) {
  var options = {
    id: [req.params.id_type, req.params.id_value],
  };

  if (req.query.viewpoint_type && req.query.viewpoint_value) {
    options.viewpoint = [req.query.viewpoint_type, req.query.viewpoint_value];
  }
  if (req.query.max_distance) { options.maxDistance = parseInt(req.query.max_distance); }

  if (req.query.type) {
    options.searchedTypes = [req.query.type];
  }
  db.getConnectedIdentifiers(options).then(function(dbRes) {
    res.json(dbRes);
  }).catch(function(err) { handleError(err, req, res); });
});



router.get('/id/:id_type/:id_value/connecting_msgs', function(req, res) {
  if (!(req.query.target_type && req.query.target_value)) {
    res.status(400).json('target_type and target_value must be specified');
    return;
  }
  var options = {
    id1: [req.params.id_type, req.params.id_value],
    id2: [req.query.target_type, req.query.target_value]
  };
  db.getConnectingMessages(options).then(function(dbRes) {
    res.json(dbRes);
  }).catch(function(err) { handleError(err, req, res); });
});



router.get('/id/:id_type/:id_value/trustpaths', function(req, res) {
  if (!(req.query.target_type && req.query.target_value)) {
    res.status(400).json('target_type and target_value must be specified');
    return;
  }
  var maxLength = req.query.max_length || 5;
  var shortestOnly = req.query.max_length !== undefined;
  db.getTrustPaths([req.params.id_type, req.params.id_value], [req.query.target_type, req.query.target_value], maxLength, shortestOnly).then(function(dbRes) {
    res.json(dbRes);
  }).catch(function(err) { handleError(err, req, res); });
});


router.get('/id/:id_type/:id_value/generatetrustmap', function(req, res) {
  var depth = parseInt(req.query.depth) || 3;
  db.generateTrustMap([req.params.id_type, req.params.id_value], depth)
  .then(function(dbRes) {
    res.json(dbRes);
  }).catch(function(err) { handleError(err, req, res); });
});

// Register the routes
app.use('/api', router);

function handleMsgEvent(data) {
  var m = data;
  db.messageExists(m.hash)
  .then(function(exists) {
    if (!exists) {
      try {
        Message.verify(m);
      } catch (e) {
        log('failed to verify msg');
        return;
      }
      db.saveMessage(m).then(function() {
        emitMsg(m);
      });
    }
  });
}

function handleIncomingWebsocket(socket) {
  log('connection from ' + socket.client.conn.remoteAddress);
  if (socket.request.headers['x-accept-incoming-connections']) {
    var peer = { url: 'http://[' + socket.client.conn.remoteAddress + ']:4944/api', last_seen: new Date() };
    db.addPeer(peer).then(function() { log('saved peer ' + peer.url); });
  }

  socket.on('msg', function (data) {
    log('msg received from ' + socket.client.conn.remoteAddress + ': ' + data.hash);
    handleMsgEvent(data);
  });
}

// Handle incoming websockets
io.on('connection', handleIncomingWebsocket);

// Start the http server
server.listen(port);

function askForMorePeers(url, peersNeeded) {
  log('asking ' + url + ' for more peers');
  identifiClient.request({
    uri: url,
    apiMethod: 'peers'
  }).then(function(res) {
    for (var i = 0; i < res.length && i < peersNeeded; i++) {
      if (res[i].url) {
        db.addPeer({ url: res[i].url }).return();
      }
    }
  });
}

function getNewMessages(url, since) {
  var sinceStr = since ? ('since ' + since) : '';
  log('asking ' + url + ' for new messages ' + sinceStr);
  identifiClient.request({
    uri: url,
    apiMethod: 'messages',
    qs: {
      created_gte: since,
      limit: 100
    }
  }).then(function(res) {
    for (var i = 0; i < res.length; i++) {
      if (res[i].jws) {
        var m = res[i];
        Message.verify(m);
        db.saveMessage(m).return();
      }
    }
  });
}

function makeConnectHandler(url, lastSeen, socket) {
  return function() {
    log('Connected to ' + url);
    socket.on('msg', handleMsgEvent);
    getNewMessages(url, lastSeen);
    db.updatePeerLastSeen({ url: url, last_seen: new Date() }).return();
    db.getPeerCount().then(function(res) {
      var peersNeeded = config.maxPeerDBsize - res[0].count;
      if (peersNeeded > 0) {
        askForMorePeers(url, peersNeeded);
      }
    });
  };
}

// Websocket connect to saved peers
if (process.env.NODE_ENV !== 'test') {
  db.init.then(function() {
    return db.getPeers();
  })
  .then(function(peers) {
    for (var i = 0; i < peers.length; i++) {
      if (outgoingConnections.length >= config.maxConnectionsOut) {
        break;
      }
      log('Attempting connection to saved peer ' + peers[i].url);
      var s = identifiClient.getSocket({ url: peers[i].url, isPeer: true, options: { connect_timeout: 5000 }});
      outgoingConnections[peers[i].url] = s;
      s.on('connect', makeConnectHandler(peers[i].url, peers[i].last_seen, s));
    }
  });
}

module.exports = server;

log('Identifi server started on port ' + port);
