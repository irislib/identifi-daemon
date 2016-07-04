/*jshint unused:false*/
'use strict';
var P = require("bluebird");

function addDefaultIdentifierAttributes(db) {
  return db.table('IdentifierAttributes').insert(
    [
      { name: 'email' },
      { name: 'account' },
      { name: 'url' },
      { name: 'tel' },
      { name: 'keyID' },
      { name: 'bitcoin' },
      { name: 'identifiNode' },
      { name: 'gpg_fingerprint'},
      { name: 'gpg_keyid'}
    ]
  );
}

function addDefaultPeers(db) {
  return db.table('Peers').count('* as count')
  .then(function(res) {
    if (parseInt(res[0].count) === 0) {
      return db('Peers').insert([
        { url: 'http://seed1.identifi.org:4944/api' },
        { url: 'http://seed2.identifi.org:4944/api' },
        { url: 'http://seed3.identifi.org:4944/api' }
      ]);
    }
  });
}

function catcher(e) {
  console.error(e);
}

var init = function(db, config) {
  var queries = [];
  queries.push(db.schema.createTableIfNotExists('IdentifierAttributes', function(t) {
    t.string('name').primary();
  }).then(function() {
    return addDefaultIdentifierAttributes(db).catch(catcher);
  }).catch(catcher));

  queries.push(db.schema.createTableIfNotExists('Messages', function(t) {
    t.string('hash').unique().primary();
    t.string('jws', 10000).notNullable();
    t.timestamp('saved_at');
    t.datetime('timestamp');
    t.string('type');
    t.integer('rating');
    t.integer('max_rating');
    t.integer('min_rating');
    t.boolean('public').default(true);
    t.integer('priority').unsigned();
    t.boolean('is_latest');
    t.string('signer_keyid');
    t.index(['timestamp']);
    t.index(['type']);
  }).catch(catcher)
  .then(function() {
    db.schema.createTableIfNotExists('MessageAttributes', function(t) {
      t.string('message_hash').references('Messages.hash');
      t.string('name').notNullable();
      t.string('value').notNullable();
      t.boolean('is_recipient');
      t.index(['message_hash', 'name', 'value']);
      t.index(['message_hash', 'is_recipient']);
      t.index(['message_hash']);
      t.index(['name', 'value']);
      t.index(['value']);
      //t.index(['lower("value")'], 'lowercase_value');
      t.primary(['message_hash', 'is_recipient', 'name', 'value']);
    }).catch(catcher);
  }));

  queries.push(db.schema.createTableIfNotExists('TrustDistances', function(t) {
    t.string('start_attr_name').notNullable();
    t.string('start_attr_value').notNullable();
    t.string('end_attr_name').notNullable();
    t.string('end_attr_value').notNullable();
    t.integer('distance').notNullable();
    t.primary(['start_attr_name', 'start_attr_value', 'end_attr_name', 'end_attr_value']);
  }).catch(catcher));

  queries.push(db.schema.createTableIfNotExists('IdentityAttributes', function(t) {
    t.integer('identity_id').unsigned();
    t.string('name').notNullable();
    t.string('value').notNullable();
    t.string('viewpoint_name').notNullable();
    t.string('viewpoint_value').notNullable();
    t.integer('confirmations').unsigned();
    t.integer('refutations').unsigned();
    t.index(['identity_id']);
    t.index(['identity_id', 'name']);
    t.index(['viewpoint_name', 'viewpoint_value']);
    t.index(['name', 'viewpoint_name', 'viewpoint_value']);
    t.primary(['name', 'value', 'viewpoint_name', 'viewpoint_value']);
  }).catch(catcher));

  queries.push(db.schema.createTableIfNotExists('IdentityStats', function(t) {
    t.integer('identity_id').unsigned().primary();
    t.string('viewpoint_name').notNullable();
    t.string('viewpoint_value').notNullable();
    t.integer('distance').notNullable().default(-1);
    t.integer('positive_score').unsigned().notNullable().default(0);
    t.integer('negative_score').unsigned().notNullable().default(0);
    t.index(['viewpoint_name', 'viewpoint_value', 'distance']);
  }).catch(catcher));

  queries.push(db.schema.createTableIfNotExists('TrustIndexedAttributes', function(t) {
    t.string('name');
    t.string('value');
    t.integer('depth').unsigned().notNullable();
    t.primary(['name', 'value']);
  }).catch(catcher));

  queries.push(db.schema.createTableIfNotExists('Peers', function(t) {
    t.string('url').primary();
    t.integer('misbehaving').unsigned().notNullable().default(0);
    t.datetime('last_seen');
  }).then(function() {
    return addDefaultPeers(db).catch(catcher);
  }).catch(catcher));

  return P.all(queries);
};

module.exports = { init: init };
