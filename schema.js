'use strict';

var addDefaultUniqueIdentifierTypes = function(db) {
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
};

var checkDefaultKey = function(db) {
  return db('PrivateKeys').count('* as count')
    .then(function(res) {
      if (res[0].count === 0) {
        // create default key
      }
    });
};

var checkDefaultTrustList = function(db) {
  return db('Messages').count('* as count')
    .then(function(res) {
      if (res[0].count === 0) {
        // add default trust list as an entry point
      }
    });
};

var init = function(db) {
  return db.schema.createTable('UniqueIdentifierTypes', function(t) {
    t.string('type').primary();
  })

  .createTable('Messages', function(t) {
    t.string('hash').primary();
    t.string('jws');
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

  .createTable('MessageIdentifiers', function(t) {
    t.string('message_hash').references('Messages.hash');
    t.string('type');
    t.string('value');
    t.primary(['type', 'value', 'message_hash']);
    t.boolean('is_recipient');
  })

  .createTable('TrustDistances', function(t) {
    t.string('start_id_type').notNullable();
    t.string('start_id_value').notNullable();
    t.string('end_id_type').notNullable();
    t.string('end_id_value').notNullable();
    t.integer('distance').notNullable();
    t.primary(['start_id_type', 'start_id_value', 'end_id_type', 'end_id_value']);
  })

  .createTable('Identities', function(t) {
    t.integer('identity_id').unsigned();
    t.string('type');
    t.string('value');
    t.string('viewpoint_type');
    t.string('viewpoint_value');
    t.integer('confirmations').unsigned();
    t.integer('refutations').unsigned();
    t.primary(['type', 'value', 'viewpoint_type', 'viewpoint_value']);
  })

  .createTable('Keys', function(t) {
    t.string('pubkey').unique();
    t.string('key_id');
    t.primary(['pubkey', 'key_id']);
  })

  .createTable('PrivateKeys', function(t) {
    t.string('pubkey').references('Keys.pubkey');
    t.string('private_key', 1000);
    t.boolean('is_default');
  })

  .then(function() {
    return addDefaultUniqueIdentifierTypes(db);
  })

  .then(function() {
    return checkDefaultKey(db);
  })

  .then(function() {
    return checkDefaultTrustList(db);
  })

  .catch(function(e) {
    if (e.code !== 'SQLITE_ERROR') {
      console.error(e);
    }
  });
};

module.exports = { init: init };
