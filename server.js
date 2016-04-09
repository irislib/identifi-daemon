var P = require("bluebird");
var moment = require('moment');

var express    = require('express');
var app        = express();
var bodyParser = require('body-parser');

var Message = require('identifi-lib/message');

var os = require('os');
var fs = require('fs');
var util = require('util');

process.env.NODE_CONFIG_DIR = __dirname + '/config';
var config = require('config');

if (process.env.NODE_ENV !== 'test') {
  // Extend default config from datadir/config.json and write the result back to it
  var datadir = process.env.IDENTIFI_DATADIR || (os.homedir() + '/.identifi');
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
} catch (ex) {
  log(ex);
  process.exit(0);
}

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

var port = config.get('port');
var server;

// Routes
// =============================================================================
var router = express.Router();

router.get('/', function(req, res) {
  res.json({ message: 'Identifi API' });
});



router.get('/status', function(req, res) {
  var queries = [db.getMessageCount(), db.getIdentityCount()];
  P.all(queries).then(function(results) {
    res.json({ msgCount: results[0][0].val, identityCount: results[1][0].val });
  });
});



router.route('/peers')
  .get(function(req, res) {
    res.json("list peers");
  })

  .post(function(req, res) {
    res.json("add peer");
  })

  .delete(function(req, res) {
    res.json("remove peer");
  });


function handleError(err, req, res) {
  log(err);
  log(req);
  res.status(500).json('Server error');
}


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
    var m = Message.decode(req.body);
    db.saveMessage(m).then(function() {
      res.status(201).json(m);
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

// Start the server
server = app.listen(port);

module.exports = server;

log('Identifi server started on port ' + port);
