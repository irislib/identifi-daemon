'use strict';

function addDefaultUniqueIdentifierTypes(db) {
  return db.table('UniqueIdentifierTypes').insert(
    [
      { type: 'email' },
      { type: 'account' },
      { type: 'url' },
      { type: 'tel' },
      { type: 'keyID' },
      { type: 'bitcoin' },
      { type: 'identifiNode' }
    ]
  );
}

function addDefaultPeers(db) {
  return db.table('Peers').count('* as count')
  .then(function(res) {
    if (res[0].count === 0) {
      return db('Peers').insert([
        { url: 'http://seed1.identifi.org:4944/api' },
        { url: 'http://seed2.identifi.org:4944/api' },
        { url: 'http://seed3.identifi.org:4944/api' }
      ]);
    }
  });
}

function setSqliteMaxSize(db, config) {
  var sqliteMaxSizeMB = config.sqliteMaxSizeMB;
  if (db.client.config.dialect !== 'sqlite3') {
    return;
  }
  if (!sqliteMaxSizeMB || sqliteMaxSizeMB < 1) {
    sqliteMaxSizeMB = 100;
  }
  return db.raw('PRAGMA page_size')
  .then(function(res) {
    var maxPageCount = Math.floor(sqliteMaxSizeMB * 1000000 / res[0].page_size);
    return db.raw('PRAGMA max_page_count = ' + maxPageCount);
  });
}

function catcher(e) {
  console.error(e);
}

var init = function(db, config) {
  return db.schema.createTableIfNotExists('UniqueIdentifierTypes', function(t) {
    t.string('type').primary();
  })

  .createTableIfNotExists('Messages', function(t) {
    t.string('hash').primary();
    t.string('jws').notNullable();
    t.timestamp('saved_at');
    t.datetime('timestamp');
    t.string('type');
    t.integer('rating');
    t.integer('max_rating');
    t.integer('min_rating');
    t.boolean('public');
    t.integer('priority').unsigned();
    t.boolean('is_latest');
    t.string('signer_keyid');
  })

  .createTableIfNotExists('MessageIdentifiers', function(t) {
    t.string('message_hash').references('Messages.hash');
    t.string('type').notNullable();
    t.string('value').notNullable();
    t.boolean('is_recipient');
    t.primary(['type', 'value', 'message_hash', 'is_recipient']);
  })

  .createTableIfNotExists('TrustDistances', function(t) {
    t.string('start_id_type').notNullable();
    t.string('start_id_value').notNullable();
    t.string('end_id_type').notNullable();
    t.string('end_id_value').notNullable();
    t.integer('distance').notNullable();
    t.primary(['start_id_type', 'start_id_value', 'end_id_type', 'end_id_value']);
  })

  .createTableIfNotExists('Identities', function(t) {
    t.integer('identity_id').unsigned();
    t.string('type').notNullable();
    t.string('value').notNullable();
    t.string('viewpoint_type');
    t.string('viewpoint_value');
    t.integer('confirmations').unsigned();
    t.integer('refutations').unsigned();
    t.primary(['type', 'value', 'viewpoint_type', 'viewpoint_value']);
  })

  .createTableIfNotExists('TrustIndexedIdentifiers', function(t) {
    t.string('type');
    t.string('value');
    t.integer('depth').unsigned().notNullable();
    t.primary(['type', 'value', 'depth']);
  })

  .createTableIfNotExists('Peers', function(t) {
    t.string('url').primary();
    t.integer('misbehaving').unsigned().notNullable().default(0);
    t.datetime('last_seen');
  })

  .then(function() {
    return addDefaultUniqueIdentifierTypes(db).catch(catcher);
  })

  .then(function() {
    return addDefaultPeers(db).catch(catcher);
  })

  .then(function() {
    return setSqliteMaxSize(db, config).catch(catcher);
  })

  .catch(catcher);
};

module.exports = { init: init };
