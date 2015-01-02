/*jshint unused: false */
'use strict';
var schema = require('./schema.js');
var P = require("bluebird");

module.exports = function(knex) {
  schema.init(knex);

  var getPriority = function(message) {
    return 0;
  };

  var isLatest = function(message) {
    return false;
  };

  return {
    saveMessage: function(message) {
      var queries = [];
      queries.push(knex('Messages').insert({
        hash:           message.hash,
        signed_data:    JSON.stringify(message.signedData),
        created:        message.signedData.timestamp,
        type:           message.signedData.type || 'rating',
        rating:         message.signedData.rating || 0,
        max_rating:     message.signedData.maxRating || 0,
        min_rating:     message.signedData.minRating || 0,
        is_published:   message.isPublished,
        priority:       getPriority(message),
        is_latest:      isLatest(message),
        signer_pubkey:  message.signature.signerPubkey,
        signature:      message.signature.signature
      }));

      var i;
      for (i = 0; i < message.signedData.author.length; i++) {
        queries.push(knex('MessageIdentifiers').insert({
          message_hash: message.hash,
          type: message.signedData.author[i][0],
          value: message.signedData.author[i][1],
          is_recipient: false
        }));
      }
      for (i = 0; i < message.signedData.recipient.length; i++) {
        queries.push(knex('MessageIdentifiers').insert({
          message_hash: message.hash,
          type: message.signedData.recipient[i][0],
          value: message.signedData.recipient[i][1],
          is_recipient: true
        }));
      }
      return P.all(queries);
    },
    getMessage: function(messageHash) {
      return knex.select('*').from('Messages').where({ hash: messageHash });
    },
    dropMessage: function(messageHash) {
      return knex('Messages').where({ hash: messageHash }).del()
      .then(function() {
        return knex('MessageIdentifiers').where({ messageHash: messageHash }).del();
      });
    },

    getSent: function(sender, limit, offset, viewpoint) {
      return knex.from('Messages')
      .innerJoin('MessageIdentifiers', 'Messages.hash', 'MessageIdentifiers.message_hash')
      .where({ 'MessageIdentifiers.type': sender[0], 'MessageIdentifiers.value': sender[1], 'MessageIdentifiers.is_recipient': false });
    },
    getReceived: function(recipient, limit, offset, viewpoint) {
      return knex.from('Messages')
      .innerJoin('MessageIdentifiers', 'Messages.hash', 'MessageIdentifiers.message_hash')
      .where({ 'MessageIdentifiers.type': recipient[0], 'MessageIdentifiers.value': recipient[1], 'MessageIdentifiers.is_recipient': true });
    },
    getConnectedIdentifiers: function(id, types, limit, offset, viewpoint) {
      return [];
    },
    getConnectingMessages: function(id1, id2, limit, offset, viewpoint) {
      return [];
    },

    generateTrustMap: function(id, maxDepth) {
      return true;
    },
    identifierSearch: function(query, limit, offset, viewpoint) {
      return [];
    },
    identitySearch: function(query, limit, offset, viewpoint) {
      return [];
    },
    getTrustPaths: function(start, end, maxLength, shortestOnly) {
      return [];
    },

    getMessageCount: function() {
      return knex('Messages').count('* as val');
    }
  };
};
