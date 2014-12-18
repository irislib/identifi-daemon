'use strict';
var schema = require('./schema.js');

module.exports = function(knex) {
  schema.init(knex);

  var getPriority = function(message) {
    return 0;
  };

  return {
    saveMessage: function(message) {
      return knex('Messages').insert({
        hash: 'hash',
        signed_data: 'signed_data',
        created: 'signed_data',
        type: 'signed_data',
        rating: 0,
        max_rating: 10,
        min_rating: -10,
        is_published: true,
        priority: getPriority(message),
        is_latest: true,
        signer_pubkey: 'signed_data',
        signature: 'signed_data'
      });
    },
    getMessage: function(messageHash) {

    },
    dropMessage: function(messageHash) {

    },

    getSent: function(sender, limit, offset, viewpoint) {

    },
    getReceived: function(recipient, limit, offset, viewpoint) {

    },
    getConnectedIdentifiers: function(id, types, limit, offset, viewpoint) {

    },
    getConnectingMessages: function(id1, id2, limit, offset, viewpoint) {

    },

    generateTrustMap: function(id, maxDepth) {

    },
    identifierSearch: function(query, limit, offset, viewpoint) {
      
    },
    identitySearch: function(query, limit, offset, viewpoint) {
      
    },
    getTrustPaths: function(start, end, maxLength, shortestOnly) {

    },

    getMessageCount: function() {
      return knex('Messages').count('* as val');
    }
  };
};
