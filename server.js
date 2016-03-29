var P = require("bluebird");
var moment = require('moment');

var express    = require('express');
var app        = express();
var bodyParser = require('body-parser');

var Message = require('identifi-lib/message');

var config = require('config');

var fs = require('fs');
var util = require('util');
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
  knex = require('knex')(config.get('db'));
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



router.get('/halt', function(req, res) {
  var msg = 'Shutting down the identifi server';
  res.json(msg);
  log(msg);
  setTimeout(function () {
    server.close();
    process.exit(0);
  }, 3000);
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



router.route('/keys')
  .get(function(req, res) {
    db.listMyKeys().then(function(dbRes) {
      res.json(dbRes);
    });
  })

  .post(function(req, res) {
    db.importPrivKey(req.body.privateKey).then(function(dbRes) {
      res.json(dbRes);
    });
  })

  .delete(function(req, res) {
    res.json("remove key");
  });



router.route('/messages')
  .get(function(req, res) {
    var where = {};
    if (req.query.type) {
      where.type = req.query.type;
    }
    db.getMessages(where).then(function(dbRes) {
      res.json(dbRes);
    });
  })

  .post(function(req, res) {
    var m = Message.decode(req.body);
    db.saveMessage(m).then(function() {
      res.status(201).json(m);
    });
  });



router.route('/messages/:hash')
  .get(function(req, res) {
    db.getMessages({ hash: req.params.hash }).then(function(dbRes) {
      if (!dbRes.length) {
        return res.status(404).json('Message not found');
      }
      res.json(dbRes[0]);
    });
  })

  .delete(function(req, res) {
    db.dropMessage(req.params.hash).then(function(dbRes) {
      if (!dbRes) {
        return res.status(404).json('Message not found');
      }
      res.json('OK');
    });
  });



router.get('/id', function(req, res) {
    db.getMessageCount().then(function(dbRes) {
      res.json({ msgCount: dbRes[0].val });
    });
});



router.get('/id/:id_type/:id_value', function(req, res) {
  db.getConnectedIdentifiers([req.params.id_type, req.params.id_value], [], 10, 0, ['email', 'alice@example.com']).then(function(dbRes) {
    res.json(dbRes);
  });
});



router.get('/id/:id_type/:id_value/overview', function(req, res) {
  db.overview([req.params.id_type, req.params.id_value], ['a', 'b']).then(function(dbRes) {
    res.json(dbRes);
  });
});



// Register the routes
app.use('/api', router);

// Start the server
server = app.listen(port);

module.exports = server;

log('Identifi server started on port ' + port);
