/*jshint unused:false*/
'use strict';

function addDefaultUniqueAttributes(db) {
  return db.table('UniqueAttributes').insert(
    [
      { name: 'email' },
      { name: 'account' },
      { name: 'url' },
      { name: 'tel' },
      { name: 'keyID' },
      { name: 'bitcoin' },
      { name: 'identifiNode' }
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

function catcher(e) {
  console.error(e);
}

var init = function(db, config) {
  return db.schema.createTableIfNotExists('UniqueAttributes', function(t) {
    t.string('name').primary();
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

  .createTableIfNotExists('MessageAttributes', function(t) {
    t.string('message_hash').references('Messages.hash');
    t.string('name').notNullable();
    t.string('value').notNullable();
    t.boolean('is_recipient');
    t.primary(['name', 'value', 'message_hash', 'is_recipient']);
  })

  .createTableIfNotExists('TrustDistances', function(t) {
    t.string('start_attr_name').notNullable();
    t.string('start_attr_value').notNullable();
    t.string('end_attr_name').notNullable();
    t.string('end_attr_value').notNullable();
    t.integer('distance').notNullable();
    t.primary(['start_attr_name', 'start_attr_value', 'end_attr_name', 'end_attr_value']);
  })

  .createTableIfNotExists('IdentityAttributes', function(t) {
    t.integer('identity_id').unsigned();
    t.string('name').notNullable();
    t.string('value').notNullable();
    t.string('viewpoint_name').notNullable();
    t.string('viewpoint_value').notNullable();
    t.integer('confirmations').unsigned();
    t.integer('refutations').unsigned();
    t.primary(['name', 'value', 'viewpoint_name', 'viewpoint_value']);
  })

  .createTableIfNotExists('TrustIndexedAttributes', function(t) {
    t.string('name');
    t.string('value');
    t.integer('depth').unsigned().notNullable();
    t.primary(['name', 'value', 'depth']);
  })

  .createTableIfNotExists('Peers', function(t) {
    t.string('url').primary();
    t.integer('misbehaving').unsigned().notNullable().default(0);
    t.datetime('last_seen');
  })

  .then(function() {
    return addDefaultUniqueAttributes(db).catch(catcher);
  })

  .then(function() {
    return addDefaultPeers(db).catch(catcher);
  })

  .catch(catcher);
};

module.exports = { init: init };
