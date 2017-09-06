/* jshint latedef:nofunc */
const Promise = require('bluebird');
const moment = require('moment');
const fs = require('fs');

const express = require('express');
const session = require('express-session');

const app = express();
const server = require('http').Server(app);
const io = require('socket.io')(server).of('/api');
const bodyParser = require('body-parser');

const identifi = require('identifi-lib');

const Message = identifi.message;
const identifiClient = identifi.client;
const pkg = require('./package.json');

const osHomedir = require('os-homedir');
const path = require('path');
const util = require('util');

const keyutil = require('identifi-lib/keyutil');

const datadir = process.env.IDENTIFI_DATADIR || (`${osHomedir()}/.identifi`);
const myKey = keyutil.getDefault(datadir);

const jwt = require('express-jwt');

const authRequired = jwt({ secret: Buffer.from(myKey.public.pem) });
const authOptional = jwt({ secret: Buffer.from(myKey.public.pem), credentialsRequired: false });
const passport = require('passport');

process.env.NODE_CONFIG_DIR = `${__dirname}/config`;
const config = require('config');

if (process.env.NODE_ENV !== 'test') {
  // Extend default config from datadir/config.json and write the result back to it
  (function setConfig() {
    if (!fs.existsSync(datadir)) {
      fs.mkdirSync(datadir);
    }
    const cfgFile = `${datadir}/config.json`;
    if (fs.existsSync(cfgFile)) {
      const cfgFromFile = require(cfgFile);
      Object.assign(config, cfgFromFile);
    }
    fs.writeFileSync(cfgFile, JSON.stringify(config, null, 4), 'utf8');
    // Set some paths
    if (config.db.connection.filename) {
      config.db.connection.filename = `${datadir}/${config.db.connection.filename}`;
    }
    config.logfile = `${datadir}/${config.logfile}`;
  }());
}

const logStream = fs.createWriteStream(config.get('logfile'), { flags: 'a', encoding: 'utf8' });

function log(msg) {
  msg = `${moment.utc().format()}: ${util.format(msg)}`;
  logStream.write(`${msg}\n`);
  console.log(msg);
}

const ipfsAPI = require('ipfs-api');

let ipfs = ipfsAPI(config.get('ipfsHost'), config.get('ipfsPort').toString());
const getIpfs = ipfs.id()
  .then((res) => {
    ipfs.myId = res.id;
    log('Connected to local IPFS API');
    return ipfs;
  })
  .catch(() => {
    log('No local IPFS API found, starting embedded IPFS node');
    function loadIpfs() {
      ipfs.load((err) => {
        if (err) { throw err; }
        console.log('IPFS repo was loaded');
        if (process.env.NODE_ENV === 'test') {
          // Do not connect to peers
          return;
        }
        ipfs.goOnline((err2) => {
          if (err2) { throw err2; }
          // We have to do this manually as of ipfs 0.20.3
          ipfs.bootstrap.list((err3, res) => {
            if (err3) { return; }
            let i;
            for (i = 0; i < res.length; i += 1) {
              console.log('connecting to peer', res[i]);
              ipfs.swarm.connect(res[i])
                .catch(log);
            }
          });
        });
      });
    }

    try {
      const IpfsLib = require('ipfs');
      ipfs = new IpfsLib();

      ipfs._repo.version.exists((err, exists) => {
        if (err) { throw err; }
        if (exists) {
          loadIpfs();
        } else {
          ipfs.init({ emptyRepo: true, bits: 2048 }, (err2) => {
            if (err2) { throw err2; }
            log('IPFS repo was initialized');
            loadIpfs();
          });
        }
      });
    } catch (e) {
      log('instantiating ipfs node failed:', e);
      ipfs = null;
    }
    return ipfs;
  });

const loginOptions = [];
const outgoingConnections = {};

process.on('uncaughtException', (e) => {
  log(e);
});

// Init DB
let knex;
let db;
try {
  const dbConf = config.get('db');
  knex = require('knex')(dbConf);
  db = require('./db.js')(knex);
  server.ready = getIpfs
    .then(newIpfs => db.init(config, newIpfs).return())
    .then(() => {
      if (ipfs && ipfs.pubsub) {
        ipfs.pubsub.subscribe('identifi', ipfsMsgHandler);
      }
    });
} catch (ex) {
  log(ex);
  process.exit(0);
}

app.use(session({
  secret: require('crypto').randomBytes(16).toString('base64'),
  resave: false,
  saveUninitialized: true,
  cookie: { secure: false },
}));
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
app.use(passport.initialize());
app.use(passport.session());

// Enable CORS
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  next();
});

// Routes
// =============================================================================
const router = express.Router();

function issueToken(receivedToken1, receivedToken2, profile, done) {
  const exp = Math.floor(Date.now() / 1000) + (60 * 60 * 24 * 365);
  let idType;
  let idValue;
  if (profile.provider === 'facebook') {
    idType = 'url';
    idValue = profile.profileUrl || `https://www.facebook.com/${profile.id}`;
  } else if (profile.provider === 'twitter') {
    idType = 'url';
    idValue = `https://twitter.com/${profile._json.screen_name}`;
  } else if (profile.provider === 'google') {
    idType = 'url';
    idValue = `https://plus.google.com/${profile.id}`;
  } else {
    idType = 'account';
    idValue = `${profile.id}@${profile.provider}`;
  }
  const payload = {
    exp,
    user: {
      idType,
      idValue,
      name: profile.displayName,
    },
  };
  const token = identifiClient.getJwt(myKey.private.pem, payload);
  const user = { token };
  done(null, user);
}

function getAuthResponse(req, res) {
  res.redirect(`/#/?token=${req.user.token}`);
}

function initializePassportStrategies() {
  passport.serializeUser((user, done) => {
    done(null, user);
  });

  passport.deserializeUser((user, done) => {
    done(null, user);
  });

  if (config.passport.facebook.clientID &&
    config.passport.facebook.clientSecret) {
    loginOptions.push('facebook');
    const FacebookStrategy = require('passport-facebook').Strategy;
    passport.use(new FacebookStrategy(config.passport.facebook, issueToken));
    router.get('/auth/facebook', passport.authenticate('facebook'));
    router.get('/auth/facebook/callback', passport.authenticate('facebook', { session: false }), getAuthResponse);
  }
  if (config.passport.twitter.consumerKey &&
    config.passport.twitter.consumerSecret) {
    loginOptions.push('twitter');
    const TwitterStrategy = require('passport-twitter').Strategy;
    passport.use(new TwitterStrategy(config.passport.twitter, issueToken));
    router.get('/auth/twitter', passport.authenticate('twitter'));
    router.get('/auth/twitter/callback', passport.authenticate('twitter'), getAuthResponse);
  }
  if (config.passport.google.clientID &&
    config.passport.google.clientSecret) {
    loginOptions.push('google');
    const GoogleStrategy = require('passport-google-oauth2').Strategy;
    passport.use(new GoogleStrategy(config.passport.google, issueToken));
    router.get('/auth/google', passport.authenticate('google', { scope: 'profile' }));
    router.get('/auth/google/callback', passport.authenticate('google', { session: false }), getAuthResponse);
  }

  if (config.passport.persona.audience) {
    loginOptions.push('persona');
    const PersonaStrategy = require('passport-persona').Strategy;
    passport.use(new PersonaStrategy({ audience: config.passport.persona.audience }, issueToken));
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
  Object.keys(outgoingConnections).forEach((key) => {
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
router.get('/', (req, res) => {
  const queries = [db.getMessageCount()];
  Promise.all(queries).then((results) => {
    res.json({
      message: 'Identifi API',
      version: pkg.version,
      identifiLibVersion: identifi.VERSION,
      msgCount: results[0],
      publicKey: myKey.public.hex,
      keyID: myKey.hash,
      loginOptions,
    });
  }).catch(err => handleError(err, req, res));
});

router.route('/reindex')
  .get(authRequired, (req, res) => {
    db.addIdentityIndexToIpfs()
    // db.addDbMessagesToIpfs()
      .then(dbRes => res.json(dbRes))
      .catch(err => handleError(err, req, res));
  });


// Helper method
function getMessages(req, res, opts) {
  const options = opts || {};
  options.where = options.where || {};
  options.where.public = true;

  if (req.query.viewpoint_name && req.query.viewpoint_value) {
    options.viewpoint = [req.query.viewpoint_name, req.query.viewpoint_value];
  }
  if (req.query.max_distance) { options.maxDistance = parseInt(req.query.max_distance); }
  if (req.query.type) { options.where['Messages.type'] = req.query.type; }
  if (req.query.order_by) { options.orderBy = req.query.order_by; }
  if (req.query.distinct_author) { options.distinctAuthor = true; }
  if (req.query.direction && (req.query.direction === 'asc' || req.query.direction === 'desc')) {
    options.direction = req.query.direction;
  }
  if (req.query.limit) { options.limit = parseInt(req.query.limit); }
  if (req.query.offset) { options.offset = parseInt(req.query.offset); }
  if (req.query.timestamp_gte) { options.timestampGte = req.query.timestamp_gte; }
  if (req.query.timestamp_lte) { options.timestampLte = req.query.timestamp_lte; }
  db.getMessages(options).then((dbRes) => {
    res.json(dbRes);
  }).catch(err => handleError(err, req, res));
}


router.route('/messages')
/**
 * @api {get} /messages List messages
 * @apiName ListMessages
 * @apiGroup Messages
 *
 * @apiParam {String} [type] Message type.
  In case of rating; :positive, :neutral or :negative can be appended
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
  .get((req, res) => {
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
  .post(authOptional, (req, res) => {
    let m = req.body;

    if (!m.hash) {
      if (req.user) {
        m.author = [[req.user.user.idType, req.user.user.idValue], ['name', req.user.user.name]];
        m = Message.create(m);
        Message.sign(m, myKey.private.pem, myKey.public.hex);
      } else {
        return res.status(400).json('Invalid identifi message or unauthorized request');
      }
    }

    return db.messageExists(m.hash)
      .then((exists) => {
        if (!exists) {
          Message.verify(m);
          db.saveMessage(m)
            .then((r) => {
              res.status(201).json(r);
            });
          emitMsg(m);
          ipfs.pubsub.publish('identifi', Buffer.from(m.jws));
        } else {
          db.saveMessage(m)
            .then((r) => {
              res.status(200).json(r);
            });
        }
      }).catch(err => handleError(err, req, res));
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
  .get((req, res) => db.getMessages({ where: { public: true, hash: req.params.hash } })
    .then((dbRes) => {
      if (!dbRes.length) {
        return res.status(404).json('Message not found');
      }
      return res.json(dbRes[0]);
    }).catch(err => handleError(err, req, res)))

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
  .delete(authRequired, (req, res) => {
    if (!req.user.admin) {
      return res.sendStatus(401);
    }
    return db.dropMessage(req.params.hash).then((dbRes) => {
      if (!dbRes) {
        return res.status(404).json('Message not found');
      }
      return res.json('OK');
    }).catch(err => handleError(err, req, res));
  });


/**
   * @api {get} /identities List identities
   * @apiName ListIdentities
   * @apiGroup Identities
   *
   * @apiParam {String} [viewpoint_name="node viewpoint type"] Trust viewpoint identity pointer type
   * @apiParam {String} [viewpoint_value="node viewpoint value"] Trust viewpoint
      identity pointer value
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
router.get('/identities', (req, res) => {
  const options = {
    where: {},
  };
  if (req.query.viewpoint_name && req.query.viewpoint_value) {
    options.viewpoint = [req.query.viewpoint_name, req.query.viewpoint_value];
  }
  if (req.query.attr_name) { options.where['attr.name'] = req.query.attr_name; }
  if (req.query.search_value) { options.searchValue = req.query.search_value; }
  if (req.query.order_by) { options.orderBy = req.query.order_by; }
  if (req.query.direction && (req.query.direction === 'asc' || req.query.direction === 'desc')) { options.direction = req.query.order_by; }
  if (req.query.limit) { options.limit = parseInt(req.query.limit); }
  if (req.query.offset) { options.offset = parseInt(req.query.offset); }
  db.getIdentityAttributes(options).then((dbRes) => {
    res.json(dbRes);
  }).catch(err => handleError(err, req, res));
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
router.get('/identities/:attr_name/:attr_value', (req, res) => {
  const options = {
    id: [req.params.attr_name, req.params.attr_value],
  };

  if (req.query.viewpoint_name && req.query.viewpoint_value) {
    options.viewpoint = [req.query.viewpoint_name, req.query.viewpoint_value];
  }
  if (req.query.max_distance) { options.maxDistance = parseInt(req.query.max_distance); }

  if (req.query.type) {
    options.searchedAttributes = [req.query.type];
  }
  db.getIdentityAttributes(options).then((dbRes) => {
    if (dbRes.length && (dbRes[0].length > 1)) {
      return res.json(dbRes[0]);
    }
    return db.mapIdentityAttributes(options)
      .then(dbRes2 => res.json(dbRes2))
      .catch(err => handleError(err, req, res));
  });
});


/**
 * @api {get} /identities/:pointer_type/:pointer_value/stats Identity stats
 * @apiName GetIdentityStats
 * @apiGroup Identities
 *
 * @apiSuccess {Number} received_positive Number of received positive ratings
 * @apiSuccess {Number} received_neutral Number of received neutral ratings
 * @apiSuccess {Number} received_negative Number of received negative ratings
 * @apiSuccess {Number} sent_positive Number of sent positive ratings
 * @apiSuccess {Number} sent_neutral Number of sent neutral ratings
 * @apiSuccess {Number} sent_negative Number of sent negative ratings
 * @apiSuccess {String} last_seen ISO timestamp of the earliest message the identity was seen in
 *
 */
router.get('/identities/:attr_name/:attr_value/stats', (req, res) => {
  const options = {};

  if (req.query.viewpoint_name && req.query.viewpoint_value) {
    options.viewpoint = [req.query.viewpoint_name, req.query.viewpoint_value];
  }
  if (req.query.max_distance) { options.maxDistance = parseInt(req.query.max_distance); }

  db.getStats([req.params.attr_name, req.params.attr_value], options)
    .then(dbRes => res.json(dbRes))
    .catch(err => handleError(err, req, res));
});


/**
 * @api {get} /identities/:pointer_type/:pointer_value/sent Messages sent by
 * @apiGroup Identities
 *
 */
router.get('/identities/:attr_name/:attr_value/sent', (req, res) => {
  const options = {
    author: [req.params.attr_name, req.params.attr_value],
  };
  getMessages(req, res, options);
});


/**
 * @api {get} /identities/:pointer_type/:pointer_value/received Messages received by
 * @apiGroup Identities
 */
router.get('/identities/:attr_name/:attr_value/received', (req, res) => {
  const options = {
    recipient: [req.params.attr_name, req.params.attr_value],
  };
  getMessages(req, res, options);
});


router.get('/identities/:attr_name/:attr_value/trustpaths', (req, res) => {
  if (!(req.query.target_name && req.query.target_value)) {
    res.status(400).json('target_name and target_value must be specified');
    return;
  }
  const maxLength = req.query.max_length || 4;
  const shortestOnly = req.query.max_length !== undefined;
  const viewpoint = ['keyID', myKey.hash];
  const limit = req.query.limit || 50;
  return db.getTrustPaths(
    [req.params.attr_name, req.params.attr_value],
    [req.query.target_name, req.query.target_value], maxLength, shortestOnly, viewpoint, limit,
  )
    .then(dbRes => res.json(dbRes))
    .catch(err => handleError(err, req, res));
});


/**
 * @apiIgnore
 * @api {get} /identities/:pointer_type/:pointer_value/stats Generatewotindex
 * @apiGroup Identities
 */
router.get('/identities/:attr_name/:attr_value/generatewotindex', authRequired, (req, res) => {
  if (!req.user.admin) {
    return res.sendStatus(401);
  }
  const depth = parseInt(req.query.depth) || 3;
  let trustedKeyID = null;
  if (req.query.trusted_keyid) {
    trustedKeyID = req.params.trusted_keyid;
  } else if (req.params.attr_name !== 'keyID') {
    trustedKeyID = myKey.hash;
  }
  const maintain = parseInt(req.query.maintain) === 1;
  let wotSize = 0;
  return db.generateWebOfTrustIndex(
    [req.params.attr_name, req.params.attr_value], depth,
    maintain, trustedKeyID,
  )
    .then((r) => {
      wotSize = r;
      return db.generateIdentityIndex([req.params.attr_name, req.params.attr_value], trustedKeyID);
    })
    .then(() => {
      res.json(wotSize);
      if (req.params.attr_name === 'keyID' && req.params.attr_value === myKey.hash && process.env.NODE_ENV !== 'test') {
        db.addIndexesToIpfs();
      }
    })
    .catch(err => handleError(err, req, res));
});

// Register the routes
app.use('/api', router);

app.get('/ipfs/:hash', (req, res) => {
  if (!ipfs) {
    return res.status(503).json('ipfs proxy not available');
  }
  return ipfs.files.cat(req.params.hash)
    .then(stream => stream.pipe(res))
    .catch(() => res.status(404).json('not found'));
});

app.use('/apidoc', express.static('./apidoc'));
// Serve identifi-angular if the node module is available
try {
  const angular = path.dirname(require.resolve('identifi-angular'));
  app.use('/', express.static(`${angular}/dist`));
} catch (e) {
  // console.log(e);
}

function handleMsgEvent(data) {
  const m = data;
  try {
    Message.verify(m);
  } catch (e) {
    log('failed to verify msg');
    return;
  }
  db.messageExists(m.hash)
    .then((exists) => {
      if (!exists) {
        db.saveMessage(m).then(() => {
          emitMsg(m);
        }).catch((e) => {
          console.log(e.stack);
          log('error handling msg', m.hash, e);
        });
      }
    });
}

function ipfsMsgHandler(msg) {
  if (msg.from !== ipfs.myId) {
    handleMsgEvent({ jws: msg.data.toString() });
  }
}

function handleIncomingWebsocket(socket) {
  log(`connection from ${socket.client.conn.remoteAddress}`);

  socket.on('msg', (data) => {
    log(`msg received from ${socket.client.conn.remoteAddress}: ${data.hash || data.ipfs_hash}`);
    handleMsgEvent(data);
  });
}

// Handle incoming websockets
io.on('connection', handleIncomingWebsocket);

// Start the http server
server.ready.then(() => {
  server.listen(config.get('port'), config.get('host'));
  log(`Identifi server started on port ${config.get('port')}`);
});

module.exports = server;
