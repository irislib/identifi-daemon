var P = require("bluebird");

var express    = require('express');
var app        = express();
var bodyParser = require('body-parser');

var knex = require('knex')({
  dialect: 'sqlite3',
  connection: {
    filename: './data.db'
  }
});
var db = require('./db.js')(knex);

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

var port = process.env.PORT || 8080;

// Routes
// =============================================================================
var router = express.Router();

router.get('/', function(req, res) {
    res.json({ message: 'identifi API' });   
});

router.get('/info', function(req, res) {
    var queries = [db.getMessageCount(), db.getIdentityCount()];
    P.all(queries).then(function(results) {
      res.json({ msgCount: results[0][0].val, identityCount: results[1][0].val });
    });
});

router.get('/msg', function(req, res) {
    db.getMessageCount().map(function(dbRes) {
      res.json({ msgCount: dbRes.val });   
    });
});

router.get('/id', function(req, res) {
    db.getMessageCount().map(function(dbRes) {
      res.json({ msgCount: dbRes.val });   
    });
});

router.get('/id/:id_type/:id_value', function(req, res) {
    db.getConnectedIdentifiers([req.params.id_type, req.params.id_value], [], 10, 0, ['email', 'alice@example.com']).map(function(dbRes) {
      res.json(dbRes.val); 
    });
});

// Register the routes
app.use('/api', router);

// Start the server
app.listen(port);
console.log('Identifi server started on port ' + port);
