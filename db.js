'use strict';
var schema = require('./schema.js');

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
      console.log(JSON.stringify(message));
      return knex('Messages').insert({
        hash:           message.hash,
        signed_data:    JSON.stringify(message.signedData),
        created:        message.signedData.timestamp || 0,
        type:           message.signedData.type || 'rating',
        rating:         message.signedData.rating || 0,
        max_rating:     message.signedData.maxRating || 0,
        min_rating:     message.signedData.minRating || 0,
        is_published:   message.isPublished,
        priority:       getPriority(message),
        is_latest:      isLatest(message),
        signer_pubkey:  message.signature.signerPubkey,
        signature:      message.signature.signature
      })
      .then(function(res) {
        var i;
        for (i = 0; i < message.signedData.authors.length; i++) {
          knex('MessageIdentifiers').insert({
            message_hash: message.hash,
            type: message.signedData.authors[i][0],
            value: message.signedData.authors[i][1],
            is_recipient: false
          });
        }
        for (i = 0; i < message.signedData.recipients.length; i++) {
          knex('MessageIdentifiers').insert({
            message_hash: message.hash,
            type: message.signedData.recipients[i][0],
            value: message.signedData.recipients[i][1],
            is_recipient: true
          });
        }
      });
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
      return [];
    },
    getReceived: function(recipient, limit, offset, viewpoint) {
      return [];
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
