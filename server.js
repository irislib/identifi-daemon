var P = require("bluebird");
var moment = require('moment');

var express    = require('express');
var session = require('express-session');
var app        = express();
var server = require('http').Server(app);
var io = require('socket.io')(server).of('/api');
var bodyParser = require('body-parser');

var identifi = require('identifi-lib');
var Message = identifi.message;
var identifiClient = identifi.client;
var pkg = require('./package.json');

var osHomedir = require('os-homedir');
var fs = require('fs');
var path = require('path');
var util = require('util');

var keyutil = require('identifi-lib/keyutil');
var datadir = process.env.IDENTIFI_DATADIR || (osHomedir() + '/.identifi');
var myKey = keyutil.getDefault(datadir);

var jwt = require('express-jwt');
var authRequired = jwt({ secret: new Buffer(myKey.public.pem) });
var authOptional = jwt({ secret: new Buffer(myKey.public.pem), credentialsRequired: false });
var passport = require('passport');

var loginOptions = [];

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
  server.ready = db.init(config).return();
} catch (ex) {
  log(ex);
  process.exit(0);
}

app.use(session({
  secret: require('crypto').randomBytes(16).toString('base64'),
  resave: false,
  saveUninitialized: true,
  cookie: { secure: false }
}));
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
app.use(passport.initialize());
app.use(passport.session());

var port = config.get('port');

// Routes
// =============================================================================
var router = express.Router();

function issueToken(receivedToken1, receivedToken2, profile, done) {
  var exp = Math.floor(Date.now() / 1000) + 60 * 60 * 24 * 365;
  var idType, idValue;
  if (profile.provider === 'facebook') {
    idType = 'url';
    idValue = profile.profileUrl || 'https://www.facebook.com/' + profile.id;
  } else if (profile.provider === 'twitter') {
    idType = 'url';
    idValue = 'https://twitter.com/' + profile._json.screen_name;
  } else if (profile.provider === 'google') {
    idType = 'url';
    idValue = 'https://plus.google.com/' + profile.id;
  } else {
    idType = 'account';
    idValue = profile.id + '@' + profile.provider;
  }
  var payload = {
    exp: exp,
    user: {
      idType: idType,
      idValue: idValue,
      name: profile.displayName
    }
  };
  var token = identifiClient.getJwt(myKey.private.pem, payload);
  var user = { token: token };
  done(null, user);
}

function getAuthResponse(req, res) {
  res.redirect('/#/?token=' + req.user.token);
}

function initializePassportStrategies() {
  passport.serializeUser(function(user, done) {
    done(null, user);
  });

  passport.deserializeUser(function(user, done) {
    done(null, user);
  });

  if (config.passport.facebook.clientID &&
    config.passport.facebook.clientSecret) {
    loginOptions.push('facebook');
    var FacebookStrategy = require('passport-facebook').Strategy;
    passport.use(new FacebookStrategy(config.passport.facebook, issueToken));
    router.get('/auth/facebook', passport.authenticate('facebook'));
    router.get('/auth/facebook/callback', passport.authenticate('facebook', { session: false }), getAuthResponse);
  }
  if (config.passport.twitter.consumerKey &&
    config.passport.twitter.consumerSecret) {
    loginOptions.push('twitter');
    var TwitterStrategy = require('passport-twitter').Strategy;
    passport.use(new TwitterStrategy(config.passport.twitter, issueToken));
    router.get('/auth/twitter', passport.authenticate('twitter'));
    router.get('/auth/twitter/callback', passport.authenticate('twitter'), getAuthResponse);
  }
  if (config.passport.google.clientID &&
    config.passport.google.clientSecret) {
    loginOptions.push('google');
    var GoogleStrategy = require('passport-google-oauth2').Strategy;
    passport.use(new GoogleStrategy(config.passport.google, issueToken));
    router.get('/auth/google', passport.authenticate('google', { scope: 'profile' }));
    router.get('/auth/google/callback', passport.authenticate('google', { session: false }), getAuthResponse);
  }

  if (config.passport.persona.audience) {
    loginOptions.push('persona');
    var PersonaStrategy = require('passport-persona').Strategy;
    passport.use(new PersonaStrategy({ audience: config.passport.persona.audience}, issueToken));
    router.post('/auth/browserid', passport.authenticate('persona', { session: false }), getAuthResponse);
  }
}

initializePassportStrategies();

function handleError(err, req, res) {
  log(err);
  log(req);
  res.status(500).json('Server error');
}


function emitMsg(msg) {
  if (typeof msg.signedData !== 'object' || msg.signedData.public === false) {
    return;
  }
  io.emit('msg', { jws: msg.jws, hash: msg.hash });
  Object.keys(outgoingConnections).forEach(function(key) {
    outgoingConnections[key].emit('msg', { jws: msg.jws, hash: msg.hash });
  });
}


/**
 * @api {get} / Identifi node information
 * @apiName Api
 * @apiGroup Api
 *
 * @apiSuccess {String} version Identifi daemon version
 * @apiSuccess {String} identifiLibVersion  Identifi library version
 * @apiSuccess {Number} msgCount  Number of Identifi messages stored on the node
 * @apiSuccess {String} publicKey DER-encoded public key of the node in hex
 * @apiSuccess {String} keyID Base 64 encoded hash of the public key
 * @apiSuccess {Array} loginOptions Array of browser login options provided by the node
 */
router.get('/', function(req, res) {
  var queries = [db.getMessageCount()];
  P.all(queries).then(function(results) {
    res.json({ message: 'Identifi API',
                version: pkg.version,
                identifiLibVersion: identifi.VERSION,
                msgCount: results[0],
                publicKey: myKey.public.hex,
                keyID: myKey.hash,
                loginOptions: loginOptions
              });
  }).catch(function(err) { handleError(err, req, res); });
});


/**
 * @api {get} /peers List peers
 * @apiName ListPeers
 * @apiGroup Peers
 */
router.route('/peers')
  .get(function(req, res) {
    db.getPeers()
    .then(function(dbRes) {
      res.json(dbRes);
    }).catch(function(err) { handleError(err, req, res); });
  })

  .post(authRequired, function(req, res) {
    if (!req.user.admin) {
      return res.sendStatus(401);
    }
    res.json("add peer");
  })

  .delete(authRequired, function(req, res) {
    if (!req.user.admin) {
      return res.sendStatus(401);
    }
    res.json("remove peer");
  });


// Helper method
function getMessages(req, res, options) {
    options = options || {};
    options.where = options.where || {};
    options.where.public = true;

    if (req.query.viewpoint_name && req.query.viewpoint_value) {
      options.viewpoint = [req.query.viewpoint_name, req.query.viewpoint_value];
    }
    if (req.query.max_distance) { options.maxDistance = parseInt(req.query.max_distance); }
    if (req.query.type)     { options.where['Messages.type'] = req.query.type; }
    if (req.query.order_by) { options.orderBy = req.query.order_by; }
    if (req.query.distinct_author) { options.distinctAuthor = true; }
    if (req.query.direction && (req.query.direction === 'asc' || req.query.direction === 'desc')) {
       options.direction = req.query.direction;
    }
    if (req.query.limit)    { options.limit = parseInt(req.query.limit); }
    if (req.query.offset)   { options.offset = parseInt(req.query.offset); }
    if (req.query.timestamp_gte)   { options.timestampGte = req.query.timestamp_gte; }
    if (req.query.timestamp_lte)   { options.timestampLte = req.query.timestamp_lte; }
    db.getMessages(options).then(function(dbRes) {
      res.json(dbRes);
    }).catch(function(err) { handleError(err, req, res); });
}


router.route('/messages')
/**
 * @api {get} /messages List messages
 * @apiName ListMessages
 * @apiGroup Messages
 *
 * @apiParam {String} [type] Message type. In case of rating; :positive, :neutral or :negative can be appended
 * @apiParam {Number} [offset=0] Offset
 * @apiParam {Number} [limit=100] Limit the number of results
 * @apiParam {String} [viewpoint_name] Trust viewpoint identity pointer type
 * @apiParam {String} [viewpoint_value] Trust viewpoint identity pointer value
 * @apiParam {Number} [max_distance] Maximum trust distance of the message author from viewpoint
 * @apiParam {String} [order_by=timestamp] Order by field
 * @apiParam {String="asc","desc"} [direction=desc] Order_by direction
 * @apiParam {Number} [timestamp_gte] Limit by timestamp greater than or equal
 * @apiParam {Number} [timestamp_lte] Limit by timestamp less than or equal
 *
 *
 */
  .get(function(req, res) {
    getMessages(req, res);
  })

/**
 * @api {post} /messages Post message
 * @apiName PostMessage
 * @apiGroup Messages
 *
 * @apiParam {String} jws Identifi message as JSON web signature
 *
 * @apiDescription
 * Successfully posted messages are broadcast to other nodes via /api websocket.
 *
 */
  .post(authOptional, function(req, res) {
    var m = req.body;

    if (!m.hash) {
      if (req.user) {
        m.author = [[req.user.user.idType, req.user.user.idValue], ['name', req.user.user.name]];
        m = Message.create(m);
        Message.sign(m, myKey.private.pem, myKey.public.hex);
      } else {
        return res.status(400).json('Invalid identifi message or unauthorized request');
      }
    }

    db.messageExists(m.hash)
    .then(function(exists) {
      if (!exists) {
        Message.verify(m);
        db.saveMessage(m).then(function() {
          res.status(201).json(m);
        });
        emitMsg(m);
      } else {
        res.status(200).json(m);
      }
    }).catch(function(err) { handleError(err, req, res); });
  });


  /**
   * @api {get} /messages/:hash Get message
   * @apiName GetMessage
   * @apiGroup Messages
   *
   * @apiDescription Get message by hash
   *
   */
router.route('/messages/:hash')
  .get(function(req, res) {
    db.getMessages({ where: { public: true, hash: req.params.hash } }).then(function(dbRes) {
      if (!dbRes.length) {
        return res.status(404).json('Message not found');
      }
      res.json(dbRes[0]);
    }).catch(function(err) { handleError(err, req, res); });
  })

  /**
   * @api {delete} /messages/:hash Delete message
   * @apiName DeleteMessage
   * @apiGroup Messages
   *
   * @apiPermission admin
   * @apiSampleRequest off
   * @apiDescription Get message by hash
   *
   */
  .delete(authRequired, function(req, res) {
    if (!req.user.admin) {
      return res.sendStatus(401);
    }
    db.dropMessage(req.params.hash).then(function(dbRes) {
      if (!dbRes) {
        return res.status(404).json('Message not found');
      }
      res.json('OK');
    }).catch(function(err) { handleError(err, req, res); });
  });


  /**
   * @api {get} /identities List identities
   * @apiName ListIdentities
   * @apiGroup Identities
   *
   * @apiParam {String} [viewpoint_name="node viewpoint type"] Trust viewpoint identity pointer type
   * @apiParam {String} [viewpoint_value="node viewpoint value"] Trust viewpoint identity pointer value
   * @apiParam {Number} [limit=100] Limit the number of results
   * @apiParam {Number} [offset=0] Offset
   * @apiParam {String} [search_value] Search identities by attribute value
   * @apiParam {String} [order_by] Order by field
   * @apiParam {String="asc","desc"} [direction] Order by direction
   *
   * @apiDescription
   * Returns an array of attribute-arrays that form identities.
   *
   */
router.get('/identities', function(req, res) {
    var options = {
      where: {},
    };
    if (req.query.viewpoint_name && req.query.viewpoint_value) {
      options.viewpoint = [req.query.viewpoint_name, req.query.viewpoint_value];
    }
    if (req.query.attr_name)        { options.where['attr.name'] = req.query.attr_name; }
    if (req.query.search_value)     { options.searchValue = req.query.search_value; }
    if (req.query.order_by)         { options.orderBy = req.query.order_by; }
    if (req.query.direction && (req.query.direction === 'asc' || req.query.direction === 'desc'))
                                    { options.direction = req.query.order_by; }
    if (req.query.limit)            { options.limit = parseInt(req.query.limit); }
    if (req.query.offset)           { options.offset = parseInt(req.query.offset); }
    db.getIdentityAttributes(options).then(function(dbRes) {
      res.json(dbRes);
    }).catch(function(err) { handleError(err, req, res); });
});


/**
 * @api {get} /identities/:pointer_type/:pointer_value Identity attributes
 * @apiName GetIdentityAttributes
 * @apiGroup Identities
 *
 * @apiDescription
 * Identifi identities are refered to by pointers of unique type, such as "email",
 * "url", "bitcoin" or "keyID".
 *
 * This method returns other identifiers and attributes that are connected to the pointer.
 *
 */
router.get('/identities/:attr_name/:attr_value', function(req, res) {
  db.getIdentityAttributes({ where: { 'attr.name': req.params.attr_name, 'attr.value': req.params.attr_value } }).then(function(dbRes) {
    res.json(dbRes);
  }).catch(function(err) { handleError(err, req, res); });
});


/**
 * @api {get} /identities/:pointer_type/:pointer_value/stats Identity stats
 * @apiName GetIdentityStats
 * @apiGroup Identities
 */
router.get('/identities/:attr_name/:attr_value/stats', function(req, res) {
  var options = {};

  if (req.query.viewpoint_name && req.query.viewpoint_value) {
    options.viewpoint = [req.query.viewpoint_name, req.query.viewpoint_value];
  }
  if (req.query.max_distance) { options.maxDistance = parseInt(req.query.max_distance); }

  db.getStats([req.params.attr_name, req.params.attr_value], options).then(function(dbRes) {
    res.json(dbRes);
  }).catch(function(err) { handleError(err, req, res); });
});


/**
 * @api {get} /identities/:pointer_type/:pointer_value/sent Messages sent by
 * @apiGroup Identities
 *
 */
router.get('/identities/:attr_name/:attr_value/sent', function(req, res) {
  var options = {
    author: [req.params.attr_name, req.params.attr_value],
  };
  getMessages(req, res, options);
});


/**
 * @api {get} /identities/:pointer_type/:pointer_value/received Messages received by
 * @apiGroup Identities
 */
router.get('/identities/:attr_name/:attr_value/received', function(req, res) {
  var options = {
    recipient: [req.params.attr_name, req.params.attr_value],
  };
  getMessages(req, res, options);
});



/**
 * @api {get} /identities/:pointer_type/:pointer_value/verifications Identity verifications
 * @apiName GetIdentityVerifications
 * @apiGroup Identities
 */
router.get('/identities/:attr_name/:attr_value/verifications', function(req, res) {
  var options = {
    id: [req.params.attr_name, req.params.attr_value],
  };

  if (req.query.viewpoint_name && req.query.viewpoint_value) {
    options.viewpoint = [req.query.viewpoint_name, req.query.viewpoint_value];
  }
  if (req.query.max_distance) { options.maxDistance = parseInt(req.query.max_distance); }

  if (req.query.type) {
    options.searchedAttributes = [req.query.type];
  }
  db.mapIdentityAttributes(options).then(function(dbRes) {
    res.json(dbRes);
  }).catch(function(err) { handleError(err, req, res); });
});



/**
 * @api {get} /identities/:pointer_type/:pointer_value/connecting_msgs Connecting messages
 * @apiGroup Identities
 */
router.get('/identities/:attr_name/:attr_value/connecting_msgs', function(req, res) {
  if (!(req.query.target_name && req.query.target_value)) {
    res.status(400).json('target_name and target_value must be specified');
    return;
  }
  var options = {
    attr1: [req.params.attr_name, req.params.attr_value],
    attr2: [req.query.target_name, req.query.target_value]
  };
  db.getConnectingMessages(options).then(function(dbRes) {
    res.json(dbRes);
  }).catch(function(err) { handleError(err, req, res); });
});


router.get('/identities/:attr_name/:attr_value/trustpaths', function(req, res) {
  if (!(req.query.target_name && req.query.target_value)) {
    res.status(400).json('target_name and target_value must be specified');
    return;
  }
  var maxLength = req.query.max_length || 4;
  var shortestOnly = req.query.max_length !== undefined;
  var viewpoint = ['keyID', myKey.hash];
  var limit = req.query.limit || 50;
  db.getTrustPaths([req.params.attr_name, req.params.attr_value], [req.query.target_name, req.query.target_value], maxLength, shortestOnly, viewpoint, limit).then(function(dbRes) {
    res.json(dbRes);
  }).catch(function(err) { handleError(err, req, res); });
});


/**
 * @apiIgnore
 * @api {get} /identities/:pointer_type/:pointer_value/stats Generatewotindex
 * @apiGroup Identities
 */
router.get('/identities/:attr_name/:attr_value/generatewotindex', authRequired, function(req, res) {
  if (!req.user.admin) {
    return res.sendStatus(401);
  }
  var depth = parseInt(req.query.depth) || 3;
  var trustedKeyID = null;
  if (req.params.trusted_keyid) {
    trustedKeyID = req.params.trusted_keyid;
  } else if (req.params.attr_name !== 'keyID') {
    trustedKeyID = myKey.hash;
  }
  db.generateWebOfTrustIndex([req.params.attr_name, req.params.attr_value], depth, false, trustedKeyID)
  .then(function(dbRes) {
    res.json(dbRes);
  }).catch(function(err) { handleError(err, req, res); });
});

// Register the routes
app.use('/api', router);

app.use('/apidoc', express.static('./apidoc'));
// Serve identifi-angular if the node module is available
try {
  var angular = path.dirname(require.resolve('identifi-angular'));
  app.use('/', express.static(angular + '/dist'));
} catch (e) {
  // console.log(e);
}

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

/* TODO: prevent infinite loops & peer misbehavior. Should maybe request by trust distance. */
function requestMessages(url, qs) {
  return identifiClient.request({
    uri: url,
    apiMethod: 'messages',
    qs: qs
  }).then(function(res) {
    for (var i = 0; i < res.length; i++) {
      if (res[i].jws) {
        var m = res[i];
        Message.verify(m);
        db.saveMessage(m).return();
      }
    }
    if (res.length === qs.limit) {
      qs.offset += qs.limit;
      return requestMessages(url, qs);
    }
  });
}

function getNewMessages(url, since) {
  var sinceStr = since ? ('since ' + since) : '';
  log('asking ' + url + ' for new messages ' + sinceStr);
  var qs = {
    timestamp_gte: since,
    limit: 100,
    offset: 0
  };
  return requestMessages(url, qs);
}

function makeConnectHandler(url, lastSeen, socket) {
  return function() {
    log('Connected to ' + url);
    socket.on('msg', handleMsgEvent);
    getNewMessages(url, lastSeen);
    db.updatePeerLastSeen({ url: url, last_seen: new Date() }).return();
    db.getPeerCount().then(function(count) {
      var peersNeeded = config.maxPeerDBsize - count;
      if (peersNeeded > 0) {
        askForMorePeers(url, peersNeeded);
      }
    });
  };
}

// Websocket connect to saved peers
if (process.env.NODE_ENV !== 'test') {
  server.ready.then(function() {
    return db.getPeers();
  })
  .then(function(peers) {
    for (var i = 0; i < peers.length; i++) {
      if (Object.keys(outgoingConnections).length >= config.maxConnectionsOut) {
        break;
      }
      log('Attempting connection to saved peer ' + peers[i].url);
      var s = identifiClient.getSocket({ url: peers[i].url, isPeer: true, options: { connect_timeout: 5000 }});
      outgoingConnections[peers[i].url] = s;
      s.on('connect', makeConnectHandler(peers[i].url, peers[i].last_seen, s));
    }
  });
}

// Start the http server
server.ready.then(function() {
  server.listen(port);
  log('Identifi server started on port ' + port);
});

module.exports = server;
