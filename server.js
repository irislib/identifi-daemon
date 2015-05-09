var P = require("bluebird");

var express    = require('express');
var app        = express();
var bodyParser = require('body-parser');

var Message = require('./message.js');

var config = require('config');

var fs = require('fs');
var util = require('util');
var logStream = fs.createWriteStream(config.get('logfile'), {flags: 'w', encoding: 'utf8'});
function log(msg) {
  msg = util.format(msg); 
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

var port = process.env.PORT || 8080;
var server;

// Routes
// =============================================================================
var router = express.Router();

router.get('/', function(req, res) {
  res.json({ message: 'Identifi API' });   
});



router.get('/info', function(req, res) {
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



router.route('/msg')
  .get(function(req, res) {
    db.getMessageCount().then(function(dbRes) {
      res.json(dbRes);
      log("list messages");
    });
  })

  .post(function(req, res) {
    var message = { signedData: req.body.messageData };
    Message.sign(message);
    db.saveMessage(message).then(function(dbRes) {
      res.json(dbRes);
    });
  });



router.route('/msg/:hash')
  .get(function(req, res) {
    res.json("get a message by hash");
  })

  .delete(function(req, res) {
    res.json("delete a message");
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
log('Identifi server started on port ' + port);
