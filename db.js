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
    return true;
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

      var sql = "DELETE FROM Identities WHERE identity_id IN ";
      knex('Identities')
        .whereRaw('identity_id IN (SELECT identity_id FROM Identities WHERE Type = ? AND Identifier = ? AND ViewpointType = ? AND ViewpointID = ?)');
      sql += "";

      var countBefore = knex('Identities').count('* as count');

      sql = "WITH RECURSIVE transitive_closure(id1type, id1val, id2type, id2val, distance, path_string, confirmations, refutations) AS ";
      sql += "( ";
      sql += "SELECT id1.Type, id1.Identifier, id2.Type, id2.Identifier, 1 AS distance, ";
      sql += "printf('%s:%s:%s:%s:',replace(id1.Type,':','::'),replace(id1.Identifier,':','::'),replace(id2.Type,':','::'),replace(id2.Identifier,':','::')) AS path_string, ";
      sql += "SUM(CASE WHEN p.Type = 'confirm_connection' AND id2.IsRecipient THEN 1 ELSE 0 END) AS Confirmations, ";
      sql += "SUM(CASE WHEN p.Type = 'refute_connection' AND id2.IsRecipient THEN 1 ELSE 0 END) AS Refutations ";
      sql += "FROM Messages AS p ";
      sql += "INNER JOIN MessageIdentifiers AS id1 ON p.Hash = id1.MessageHash ";
      sql += "INNER JOIN MessageIdentifiers AS id2 ON p.Hash = id2.MessageHash AND id2.IsRecipient = id1.IsRecipient AND (id1.Type != id2.Type OR id1.Identifier != id2.Identifier) ";
      
      // AddMessageFilterSQL(sql, viewpoint, maxDistance, msgType);

      sql += "WHERE id1.Type = @type AND id1.Identifier = @id ";
      // AddMessageFilterSQLWhere(sql, viewpoint);
      sql += "GROUP BY id2.Type, id2.Identifier ";

      sql += "UNION ALL ";

      sql += "SELECT tc.id1type, tc.id1val, id2.Type, id2.Identifier, tc.distance + 1, ";
      sql += "printf('%s%s:%s:',tc.path_string,replace(id2.Type,':','::'),replace(id2.Identifier,':','::')) AS path_string, ";
      sql += "SUM(CASE WHEN p.Type = 'confirm_connection' AND id2.IsRecipient THEN 1 ELSE 0 END) AS Confirmations, ";
      sql += "SUM(CASE WHEN p.Type = 'refute_connection' AND id2.IsRecipient THEN 1 ELSE 0 END) AS Refutations ";
      sql += "FROM Messages AS p ";
      sql += "JOIN MessageIdentifiers AS id1 ON p.Hash = id1.MessageHash AND id1.IsRecipient = 1 ";
      sql += "JOIN UniqueIdentifierTypes AS tpp1 ON tpp1.Value = id1.Type ";
      sql += "JOIN MessageIdentifiers AS id2 ON p.Hash = id2.MessageHash AND id2.IsRecipient = 1 AND (id1.Type != id2.Type OR id1.Identifier != id2.Identifier) ";
      sql += "JOIN transitive_closure AS tc ON tc.confirmations > tc.refutations AND id1.Type = tc.id2type AND id1.Identifier = tc.id2val ";
      sql += "INNER JOIN UniqueIdentifierTypes AS tpp2 ON tpp2.Value = tc.id1type ";
      
      // AddMessageFilterSQL(sql, viewpoint, maxDistance, msgType);
      
      sql += "WHERE p.Type IN ('confirm_connection','refute_connections') AND tc.distance < 10 ";
      // AddMessageFilterSQLWhere(sql, viewpoint);
      sql += "AND tc.path_string NOT LIKE printf('%%%s:%s:%%',replace(id2.Type,':','::'),replace(id2.Identifier,':','::'))";
      sql += "GROUP BY id2.Type, id2.Identifier ";
      sql += ") ";

      var identityID = knex.raw("SELECT IFNULL(MAX(identity_id), 0) + 1 FROM Identities");
      sql += "INSERT INTO Identities ";
      sql += "SELECT " + identityID + ", id2type, id2val, @viewpointType, @viewpointID, SUM(confirmations), SUM(refutations) FROM transitive_closure ";
      sql += "GROUP BY id2type, id2val ";
      sql += "UNION SELECT " + identityID + ", @type, @id, @viewpointType, @viewpointID, 1, 0 ";
      sql += "FROM MessageIdentifiers AS mi ";
      sql += "INNER JOIN UniqueIdentifierTypes AS ui ON ui.Value = mi.Type ";
      sql += "WHERE mi.Type = @type AND mi.Identifier = @id  ";

      if (countBefore === knex('Identities').count('* as val')) {
        return [];
      }

      
      sql = "SELECT Type, Identifier, Confirmations AS c, Refutations AS r, 1 FROM Identities WHERE NOT (Type = @searchedtype AND Identifier = @searchedid) AND identity_id = (SELECT MAX(identity_id) FROM Identities) ";
      /*if (types && !types.empty()) {
          vector<string> questionMarks(searchedTypes.size(), "?");
          sql += "AND Type IN (" += algorithm::join(questionMarks, ", ") += ") ";
      }*/
      sql += "GROUP BY Type, Identifier ";
      sql += "ORDER BY c-r DESC ";

      return knex('Identities')
        .whereRaw('identity_id = (SELECT MAX(identity_id) FROM Identities)');
    },
    getConnectingMessages: function(id1, id2, limit, offset, viewpoint) {
      return knex.from('Messages')
        .innerJoin('MessageIdentifiers as id1', 'Messages.hash', 'id1.message_hash')
        .innerJoin('MessageIdentifiers as id2', 'id1.message_hash', 'id2.message_hash')
        .where({ 
          'id1.type': id1[0],
          'id1.value': id1[1],
          'id1.is_recipient': true,
          'id2.type': id2[0],
          'id2.value': id2[1],
          'id2.is_recipient': true
        });
    },

    generateTrustMap: function(id, maxDepth) {
      var sql = "WITH RECURSIVE transitive_closure(id1type, id1val, id2type, id2val, distance, path_string) AS ";
      sql += "(";
      sql += "SELECT id1.type, id1.value, id2.type, id2.value, 1 AS distance, "; 
      sql += "printf('%s:%s:%s:%s:',replace(id1.type,':','::'),replace(id1.value,':','::'),replace(id2.type,':','::'),replace(id2.value,':','::')) AS path_string "; 
      sql += "FROM Messages AS m "; 
      sql += "INNER JOIN MessageIdentifiers AS id1 ON m.hash = id1.message_hash AND id1.is_recipient = 0 "; 
      sql += "INNER JOIN UniqueIdentifierTypes AS uidt1 ON uidt1.type = id1.type ";
      sql += "INNER JOIN MessageIdentifiers AS id2 ON m.hash = id2.message_hash AND (id1.type != id2.type OR id1.value != id2.value) "; 
      sql += "INNER JOIN UniqueIdentifierTypes AS uidt2 ON uidt2.type = id2.type ";
      sql += "WHERE m.is_latest AND m.rating > (m.min_rating + m.max_rating) / 2 AND id1.type = @id1type AND id1.value = @id1value ";

      sql += "UNION ALL "; 

      sql += "SELECT tc.id1type, tc.id1val, id2.type, id2.value, tc.distance + 1, "; 
      sql += "printf('%s%s:%s:',tc.path_string,replace(id2.type,':','::'),replace(id2.value,':','::')) AS path_string "; 
      sql += "FROM Messages AS m "; 
      sql += "INNER JOIN MessageIdentifiers AS id1 ON m.hash = id1.message_hash AND id1.is_recipient = 0 "; 
      sql += "INNER JOIN UniqueIdentifierTypes AS uidt1 ON uidt1.type = id1.type ";
      sql += "INNER JOIN MessageIdentifiers AS id2 ON m.hash = id2.message_hash AND (id1.type != id2.type OR id1.value != id2.value) "; 
      sql += "INNER JOIN UniqueIdentifierTypes AS uidt2 ON uidt2.type = id2.type ";
      sql += "JOIN transitive_closure AS tc ON id1.type = tc.id2type AND id1.value = tc.id2val "; 
      sql += "WHERE m.is_latest AND m.rating > (m.min_rating + m.max_rating) / 2 AND tc.distance < ? AND tc.path_string NOT LIKE printf('%%%s:%s:%%',replace(id2.type,':','::'),replace(id2.value,':','::')) "; 
      sql += ") "; 
      sql += "INSERT OR REPLACE INTO TrustDistances (start_id_type, start_id_value, end_id_type, end_id_value, distance) SELECT @id1type, @id1value, id2type, id2val, distance FROM transitive_closure "; 

      return knex('TrustDistances')
        .where({ start_id_type: id[0], start_id_value: id[1] }).del()
        .then(function() {
          return knex.raw(sql, [id[0], id[1]]);
        })
        .then(function() {
          return knex('TrustDistances').count('* as val')
            .where({ start_id_type: id[0], start_id_value: id[1] });
        });
    },
    identifierSearch: function(query, limit, offset, viewpoint) {
      return knex.from('MessageIdentifiers')
        .where('value', 'like', '%' + query + '%')
        .distinct('type', 'value').select();
    },
    identitySearch: function(query, limit, offset, viewpoint) {
      return [];
    },
    getTrustPaths: function(start, end, maxLength, shortestOnly) {
      return [];
    },

    getMessageCount: function() {
      return knex('Messages').count('* as val');
    },

    importPrivateKey: function(privateKey) {
      return false;
    },
    listMyKeys: function() {
      return [];
    },
    getPrivateKey: function(keyID) {
      return;
    }
  };
};
