/*jshint unused: false */
'use strict';
var schema = require('./schema.js');
var Message = require('identifi-lib/message');
var P = require("bluebird");
var moment = require('moment');


module.exports = function(knex) {
  schema.init(knex);
  var p; // Private methods

  var publicMethods = {
    saveMessage: function(message) {
      var queries = [];

      return p.getPriority(message).then(function(priority) {
        queries.push(knex('Messages').insert({
          hash:           message.hash,
          jws:            message.jws,
          saved_at:       moment(message.signedData.timestamp).unix(),
          timestamp:      message.signedData.timestamp,
          type:           message.signedData.type || 'rating',
          rating:         message.signedData.rating || 0,
          max_rating:     message.signedData.maxRating || 0,
          min_rating:     message.signedData.minRating || 0,
          public:         message.signedData.public || true,
          priority:       priority,
          is_latest:      p.isLatest(message),
          signer_keyid:   message.jwsHeader.kid,
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
        queries.push(p.saveMessageTrustDistances(message));
        return P.all(queries);
      });
    },
    getMessages: function(options) {
      var defaultOptions = {
        orderBy: 'timestamp',
        direction: 'desc',
        limit: 100,
        offset: 0,
        where: {}
      };
      options = options || defaultOptions;
      for (var key in defaultOptions) {
        options[key] = options[key] !== undefined ? options[key] : defaultOptions[key];
      }

      if (options.author || options.recipient) {
        if (options.author) {
          options.where['id.type'] = options.author[0];
          options.where['id.value'] = options.author[1];
          options.where['id.is_recipient'] = false;
        } else {
          options.where['id.type'] = options.recipient[0];
          options.where['id.value'] = options.recipient[1];
          options.where['id.is_recipient'] = true;
        }

        if (options.where['Messages.type'] && options.where['Messages.type'].match(/^rating:(positive|neutral|negative)$/i)) {
          var ratingType = options.where['Messages.type'].match(/(positive|neutral|negative)$/i)[0];
          options.where['Messages.type'] = 'rating';
          var ratingTypeFilter;
          switch(ratingType) {
            case 'positive':
              ratingTypeFilter = ['Messages.rating', '<', '(Messages.max_rating + Messages.min_rating) / 2'];
              break;
            case 'neutral':
              ratingTypeFilter = ['Messages.rating', '=', '(Messages.max_rating + Messages.min_rating) / 2'];
              break;
            case 'negative':
              ratingTypeFilter = ['Messages.rating', '>', '(Messages.max_rating + Messages.min_rating) / 2'];
              break;
          }
          return knex.select('*').from('Messages')
            .innerJoin('MessageIdentifiers as id', 'Messages.hash', 'id.message_hash')
            .where(options.where)
            .where(ratingTypeFilter[0], ratingTypeFilter[1], ratingTypeFilter[2])
            .orderBy(options.orderBy, options.direction)
            .limit(options.limit)
            .offset(options.offset);
        }
        return knex.select('*').from('Messages')
          .innerJoin('MessageIdentifiers as id', 'Messages.hash', 'id.message_hash')
          .where(options.where)
          .orderBy(options.orderBy, options.direction)
          .limit(options.limit)
          .offset(options.offset);
      }

      return knex.select('*').from('Messages')
        .where(options.where)
        .orderBy(options.orderBy, options.direction)
        .limit(options.limit)
        .offset(options.offset);
    },

    dropMessage: function(messageHash) {
      var messageDeleted = 0;
      return knex('Messages').where({ hash: messageHash }).del()
        .then(function(res) {
          messageDeleted = res;
          return knex('MessageIdentifiers').where({ message_hash: messageHash }).del();
        }).then(function(res) {
          return new P(function(resolve) { resolve(messageDeleted ? true : false); });
        });
    },

    getIdentities: function(options) {
      var defaultOptions = {
        orderBy: 'value',
        direction: 'asc',
        limit: 100,
        offset: 0,
        where: {}
      };
      options = options || defaultOptions;
      for (var key in defaultOptions) {
        options[key] = options[key] !== undefined ? options[key] : defaultOptions[key];
      }

      if (options.searchValue) {
        return knex.from('MessageIdentifiers').distinct('type', 'value').select()
          .where(options.where)
          .where('value', 'like', '%' + options.searchValue + '%')
          .orderBy(options.orderBy, options.direction)
          .limit(options.limit)
          .offset(options.offset);
      }
      return knex.from('MessageIdentifiers').distinct('type', 'value').select()
        .where(options.where)
        .orderBy(options.orderBy, options.direction)
        .limit(options.limit)
        .offset(options.offset);
    },

    getConnectedIdentifiers: function(options) {
      var sql = 'identity_id IN (SELECT identity_id FROM Identities WHERE type = ? AND value = ? AND viewpoint_type = ? AND viewpoint_value = ?)';
      var countBefore;

      return knex('Identities')
        .whereRaw(sql, [options.id[0], options.id[1], options.viewpoint[0], options.viewpoint[1]]).del()
        .then(function() {
          return knex('Identities').count('* as count');
        })
        .then(function(res) {
          countBefore = res[0].count;
          return knex.raw("SELECT IFNULL(MAX(identity_id), 0) + 1 AS val FROM Identities");
        })
        .then(function(res) {
          var identityID = res[0].val;
          sql = "WITH RECURSIVE transitive_closure(id1type, id1val, id2type, id2val, distance, path_string, confirmations, refutations) AS ";
          sql += "( ";
          sql += "SELECT id1.type, id1.value, id2.type, id2.value, 1 AS distance, ";
          sql += "printf('%s:%s:%s:%s:',replace(id1.type,':','::'),replace(id1.value,':','::'),replace(id2.type,':','::'),replace(id2.value,':','::')) AS path_string, ";
          sql += "1 AS Confirmations, "; // TODO fix
          sql += "0 AS Refutations ";
          sql += "FROM Messages AS p ";
          sql += "INNER JOIN MessageIdentifiers AS id1 ON p.hash = id1.message_hash ";
          sql += "INNER JOIN MessageIdentifiers AS id2 ON p.hash = id2.message_hash AND id2.is_recipient = id1.is_recipient AND (id1.type != id2.type OR id1.value != id2.value) ";

          // AddMessageFilterSQL(sql, viewpoint, maxDistance, msgType);

          sql += "WHERE id1.type = :type AND id1.value = :id ";
          // AddMessageFilterSQLWhere(sql, viewpoint);

          sql += "UNION ALL ";

          sql += "SELECT tc.id1type, tc.id1val, id2.type, id2.value, tc.distance + 1, ";
          sql += "printf('%s%s:%s:',tc.path_string,replace(id2.type,':','::'),replace(id2.value,':','::')) AS path_string, ";
          sql += "1 AS Confirmations, "; // TODO fix
          sql += "0 AS Refutations ";
          sql += "FROM Messages AS p ";
          sql += "JOIN MessageIdentifiers AS id1 ON p.hash = id1.message_hash AND id1.is_recipient = 1 ";
          sql += "JOIN UniqueIdentifierTypes AS tpp1 ON tpp1.type = id1.type ";
          sql += "JOIN MessageIdentifiers AS id2 ON p.hash = id2.message_hash AND id2.is_recipient = 1 AND (id1.type != id2.type OR id1.value != id2.value) ";
          sql += "JOIN transitive_closure AS tc ON tc.confirmations > tc.refutations AND id1.type = tc.id2type AND id1.value = tc.id2val ";
          sql += "INNER JOIN UniqueIdentifierTypes AS tpp2 ON tpp2.type = tc.id1type ";

          // AddMessageFilterSQL(sql, viewpoint, maxDistance, msgType);

          sql += "WHERE p.type IN ('confirm_connection','refute_connections') AND tc.distance < 10 ";
          // AddMessageFilterSQLWhere(sql, viewpoint);
          sql += "AND tc.path_string NOT LIKE printf('%%%s:%s:%%',replace(id2.type,':','::'),replace(id2.value,':','::'))";
          sql += ") ";

          sql += "INSERT INTO Identities ";
          sql += "SELECT " + identityID + ", id2type, id2val, :viewpointType, :viewpointID, 1, 1 FROM transitive_closure ";
          sql += "GROUP BY id2type, id2val ";
          sql += "UNION SELECT " + identityID + ", :type, :id, :viewpointType, :viewpointID, 1, 0 ";
          sql += "FROM MessageIdentifiers AS mi ";
          sql += "INNER JOIN UniqueIdentifierTypes AS ui ON ui.type = mi.type ";
          sql += "WHERE mi.type = :type AND mi.value = :id  ";

          var sqlValues = {
            type: options.id[0],
            id: options.id[1],
            viewpointType: options.viewpoint[0],
            viewpointID: options.viewpoint[1]
          };

          return knex.raw(sql, sqlValues);
        }).then(function() {
          return knex('Identities').count('* as val');
        })
        .then(function(res) {
          if (countBefore === res[0].val) {
            return new P(function(resolve) { resolve([]); });
          }

          var hasSearchedTypes = options.searchedTypes && options.searchedTypes.length > 0;

          if (hasSearchedTypes) {
            return knex('Identities')
              .select('type', 'value', 'confirmations', 'refutations')
              .where(knex.raw('NOT (Type = ? AND value = ?) AND identity_id = (SELECT MAX(identity_id) FROM Identities)', [options.id[0], options.id[1]]))
              .whereIn('type', options.searchedTypes)
              .groupBy('type', 'value')
              .orderByRaw('confirmations - refutations DESC');
          }

          return knex('Identities')
            .select('type', 'value', 'confirmations', 'refutations')
            .where(knex.raw('NOT (Type = ? AND value = ?) AND identity_id = (SELECT MAX(identity_id) FROM Identities)', [options.id[0], options.id[1]]))
            .groupBy('type', 'value')
            .orderByRaw('confirmations - refutations DESC');
        });
    },

    getTrustDistance: function(id1, id2) {
      if (id1[0] === id2[0] && id1[1] === id2[1]) {
        return new P(function(resolve) { resolve(1); });
      }
      return knex.select('distance').from('TrustDistances').where({
        'start_id_type': id1[0],
        'start_id_value': id1[1],
        'end_id_type': id2[0],
        'end_id_value': id2[1]
      });
    },

    getConnectingMessages: function(options) {
      return knex.select('Messages.*').from('Messages')
        .innerJoin('MessageIdentifiers as id1', 'Messages.hash', 'id1.message_hash')
        .innerJoin('MessageIdentifiers as id2', 'id1.message_hash', 'id2.message_hash')
        .where({
          'id1.type': options.id1[0],
          'id1.value': options.id1[1],
          'id1.is_recipient': true,
          'id2.type': options.id2[0],
          'id2.value': options.id2[1],
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
      sql += "WHERE m.is_latest AND m.rating > (m.min_rating + m.max_rating) / 2 AND id1.type = :id1type AND id1.value = :id1value ";

      sql += "UNION ALL ";

      sql += "SELECT tc.id1type, tc.id1val, id2.type, id2.value, tc.distance + 1, ";
      sql += "printf('%s%s:%s:',tc.path_string,replace(id2.type,':','::'),replace(id2.value,':','::')) AS path_string ";
      sql += "FROM Messages AS m ";
      sql += "INNER JOIN MessageIdentifiers AS id1 ON m.hash = id1.message_hash AND id1.is_recipient = 0 ";
      sql += "INNER JOIN UniqueIdentifierTypes AS uidt1 ON uidt1.type = id1.type ";
      sql += "INNER JOIN MessageIdentifiers AS id2 ON m.hash = id2.message_hash AND (id1.type != id2.type OR id1.value != id2.value) ";
      sql += "INNER JOIN UniqueIdentifierTypes AS uidt2 ON uidt2.type = id2.type ";
      sql += "JOIN transitive_closure AS tc ON id1.type = tc.id2type AND id1.value = tc.id2val ";
      sql += "WHERE m.is_latest AND m.rating > (m.min_rating + m.max_rating) / 2 AND tc.distance < :maxDepth AND tc.path_string NOT LIKE printf('%%%s:%s:%%',replace(id2.type,':','::'),replace(id2.value,':','::')) ";
      sql += ") ";
      sql += "INSERT OR REPLACE INTO TrustDistances (start_id_type, start_id_value, end_id_type, end_id_value, distance) SELECT :id1type, :id1value, id2type, id2val, distance FROM transitive_closure ";

      return knex('TrustDistances')
        .where({ start_id_type: id[0], start_id_value: id[1] }).del()
        .then(function() {
          return knex.raw(sql, { id1type: id[0], id1value: id[1], maxDepth: maxDepth });
        })
        .then(function() {
          return knex('TrustDistances').count('* as val')
            .where({ start_id_type: id[0], start_id_value: id[1] });
        });
    },

    identitySearch: function(query, limit, offset, viewpoint) {
      viewpoint = viewpoint || ['', ''];
      var useViewpoint = viewpoint[0] && viewpoint[1];

      var sql = "SELECT IFNULL(OtherIdentifiers.type,idtype) AS type, IFNULL(OtherIdentifiers.value,idvalue) AS value, MAX(iid) AS iid FROM (";
      sql += "SELECT DISTINCT mi.type AS idtype, mi.value AS idvalue, -1 AS iid FROM MessageIdentifiers AS mi ";
      sql += "WHERE ";
      sql += "mi.value LIKE '%' || :query || '%' ";

      if (query[0]) {
        sql += "AND mi.type = :type ";
      }

      sql += "UNION ";
      sql += "SELECT DISTINCT ii.type AS idtype, ii.value AS idvalue, ii.identity_id AS iid FROM Identities AS ii ";
      sql += "WHERE ";
      sql += "ii.value LIKE '%' || :query || '%' ";

      if (query[0]) {
        sql += "AND ii.type = :type ";
      }

      sql += "AND viewpoint_type = :viewType AND viewpoint_value = :viewID ";
      sql += ") ";

      if (useViewpoint) {
        sql += "LEFT JOIN TrustDistances AS tp ON tp.end_type = idtype AND tp.end_value = idvalue ";
        sql += "AND tp.start_type = :viewType AND tp.start_value = :viewID ";
      }

      sql += "LEFT JOIN UniqueIdentifierTypes AS UID ON UID.type = idtype ";
      sql += "LEFT JOIN Identities AS OtherIdentifiers ON OtherIdentifiers.identity_id = iid AND OtherIdentifiers.confirmations >= OtherIdentifiers.refutations ";

      if (useViewpoint) {
        sql += "AND OtherIdentifiers.viewpoint_type = :viewType AND OtherIdentifiers.viewpoint_value = :viewID ";
      }

      //sql += "LEFT JOIN CachedNames AS cn ON cn.type = type AND cn.value = id ";
      //sql += "LEFT JOIN CachedEmails AS ce ON ce.type = type AND ce.value = id ";

      sql += "GROUP BY IFNULL(OtherIdentifiers.type,idtype), IFNULL(OtherIdentifiers.value,idvalue) ";

      if (useViewpoint) {
        sql += "ORDER BY iid >= 0 DESC, IFNULL(tp.Distance,1000) ASC, CASE WHEN idvalue LIKE :query || '%' THEN 0 ELSE 1 END, UID.type IS NOT NULL DESC, idvalue ASC ";
      }

      var params = { query: query[1], type: query[0], viewType: viewpoint[0], viewID: viewpoint[1] };
      return knex.raw(sql, params);
    },

    getTrustPaths: function(start, end, maxLength, shortestOnly) {
      var sql = '';
      sql += "WITH RECURSIVE transitive_closure(id1type, id1val, id2type, id2val, distance, path_string) AS ";
      sql += "(";
      sql += "SELECT id1.type, id1.value, id2.type, id2.value, 1 AS distance, ";
      sql += "printf('%s:%s:%s:%s:',replace(id1.type,':','::'),replace(id1.value,':','::'),replace(id2.type,':','::'),replace(id2.value,':','::')) AS path_string ";
      sql += "FROM Messages AS m ";
      sql += "INNER JOIN MessageIdentifiers AS id1 ON m.Hash = id1.message_hash AND id1.is_recipient = 0 ";
      sql += "INNER JOIN UniqueIdentifierTypes AS tpp1 ON tpp1.type = id1.type ";
      sql += "INNER JOIN MessageIdentifiers AS id2 ON m.Hash = id2.message_hash AND (id1.type != id2.type OR id1.value != id2.value) ";
      sql += "INNER JOIN UniqueIdentifierTypes AS tpp2 ON tpp2.type = id2.type ";
      sql += "WHERE m.is_latest AND m.Rating > (m.min_rating + m.max_rating) / 2 AND id1.type = ? AND id1.value = ? ";

      sql += "UNION ALL ";

      sql += "SELECT tc.id1type, tc.id1val, id2.type, id2.value, tc.distance + 1, ";
      sql += "printf('%s%s:%s:',tc.path_string,replace(id2.type,':','::'),replace(id2.value,':','::')) AS path_string ";
      sql += "FROM Messages AS m ";
      sql += "INNER JOIN MessageIdentifiers AS id1 ON m.Hash = id1.message_hash AND id1.is_recipient = 0 ";
      sql += "INNER JOIN UniqueIdentifierTypes AS tpp1 ON tpp1.type = id1.type ";
      sql += "INNER JOIN MessageIdentifiers AS id2 ON m.Hash = id2.message_hash AND (id1.type != id2.type OR id1.value != id2.value) ";
      sql += "INNER JOIN UniqueIdentifierTypes AS tpp2 ON tpp2.type = id2.type ";
      sql += "JOIN transitive_closure AS tc ON id1.type = tc.id2type AND id1.value = tc.id2val ";
      sql += "WHERE m.is_latest AND m.Rating > (m.min_rating + m.max_rating) / 2 AND tc.distance < ? AND tc.path_string NOT LIKE printf('%%%s:%s:%%',replace(id2.type,':','::'),replace(id2.value,':','::')) ";
      sql += ") ";
      sql += "SELECT DISTINCT path_string FROM transitive_closure ";
      sql += "WHERE id2type = ? AND id2val = ? ";
      sql += "ORDER BY distance ";

      return knex.raw(sql, [start[0], start[1], maxLength, end[0], end[1]]);
    },

    getMessageCount: function() {
      return knex('Messages').count('* as val');
    },

    getIdentityCount: function() {
      return knex('Identities').count('* as val');
    },

    importPrivateKey: function(privateKey, setDefault) {
      return knex('PrivateKeys').insert({
        pubkey: '',
        private_key: privateKey,
        is_default: setDefault
      }).then(function() {
        return knex('Keys').insert({
          pubkey: '',
          key_id: ''
        });
      });
    },

    listMyKeys: function() {
      return knex.select('*').from('Keys').join('PrivateKeys', 'PrivateKeys.pubkey', 'Keys.pubkey');
    },

    getStats: function(id, viewpoint) {
      var useViewpoint = false;
      var sql = "SELECT ";
      sql += "SUM(CASE WHEN pi.is_recipient = 0 AND p.rating > (p.min_rating + p.max_rating) / 2 THEN 1 ELSE 0 END) AS sentPositive, ";
      sql += "SUM(CASE WHEN pi.is_recipient = 0 AND p.rating == (p.min_rating + p.max_rating) / 2 THEN 1 ELSE 0 END) AS sentNeutral, ";
      sql += "SUM(CASE WHEN pi.is_recipient = 0 AND p.rating < (p.min_rating + p.max_rating) / 2 THEN 1 ELSE 0 END) AS sentNegative, ";
      if (!useViewpoint) {
          sql += "SUM(CASE WHEN pi.is_recipient = 1 AND p.rating > (p.min_rating + p.max_rating) / 2 THEN 1 ELSE 0 END) AS receivedPositive, ";
          sql += "SUM(CASE WHEN pi.is_recipient = 1 AND p.rating == (p.min_rating + p.max_rating) / 2 THEN 1 ELSE 0 END) AS receivedNeutral, ";
          sql += "SUM(CASE WHEN pi.is_recipient = 1 AND p.rating < (p.min_rating + p.max_rating) / 2 THEN 1 ELSE 0 END) AS receivedNegative, ";
      } else {
          sql += "SUM(CASE WHEN pi.is_recipient = 1 AND p.rating > (p.min_rating + p.max_rating) / 2 AND ";
          sql += "(tp.start_value IS NOT NULL OR (author.value = :viewpointID AND author.type = :viewpointType)) THEN 1 ELSE 0 END) AS receivedPositive, ";
          sql += "SUM(CASE WHEN pi.is_recipient = 1 AND p.rating == (p.min_rating + p.max_rating) / 2 AND ";
          sql += "(tp.start_value IS NOT NULL OR (author.value = :viewpointID AND author.type = :viewpointType)) THEN 1 ELSE 0 END) AS receivedNeutral, ";
          sql += "SUM(CASE WHEN pi.is_recipient = 1 AND p.rating < (p.min_rating + p.max_rating) / 2 AND  ";
          sql += "(tp.start_value IS NOT NULL OR (author.value = :viewpointID AND author.type = :viewpointType)) THEN 1 ELSE 0 END) AS receivedNegative, ";
      }
      sql += "MIN(p.timestamp) AS firstSeen ";
      sql += "FROM Messages AS p ";
      sql += "INNER JOIN MessageIdentifiers AS pi ON pi.message_hash = p.hash ";
      sql += "INNER JOIN UniqueIdentifierTypes AS tpp ON tpp.type = pi.type ";
      if (useViewpoint) {
          sql += "INNER JOIN Identities AS i ON pi.type = i.type AND pi.value = i.value AND i.identity_id = ";
          sql += "(SELECT identity_id FROM Identities WHERE viewpoint_value = :viewpointID AND viewpoint_type = :viewpointType ";
          sql += "AND type = :type AND value = :value) ";
      }
      //AddMessageFilterSQL(sql, viewpoint, maxDistance, msgType);
      sql += "WHERE p.type = 'rating' AND pi.type = :type AND pi.value = :value ";
      sql += "AND p.is_latest = 1 ";

      if (useViewpoint) {
          sql += "AND (tp.start_value IS NOT NULL OR (author.value = :viewpointID AND author.type = :viewpointType) ";
          sql += "OR (author.type = :type AND author.value = :value)) ";
          sql += "GROUP BY i.identity_id ";
      }

      return knex.raw(sql, { type: id[0], value: id[1], viewpointType: viewpoint[0], viewpointID: viewpoint[1] });
    }
  };

  p = {
    getPriority: function(message) {
      var maxPriority = 100;
      var keyType = 'keyID';

      var shortestPathToSignature = 1000000;

      var i, j;
      return publicMethods.listMyKeys().then(function(res) {
        var queries = [];
        for (i = 0; i < res.length; i++) {
          var key = res[i].keyID;
          queries.push(publicMethods.getTrustDistance([keyType, key], [keyType, message.jwsHeader.kid]));
        }
        return P.all(queries);
      }).then(function(res) {
        for (i = 0; i < res.length; i++) {
          if (res[i].length > 0 && res[i][0] < shortestPathToSignature) {
            shortestPathToSignature = res[i][0];
          }
        }
        return new P(function(resolve) {
          resolve(Math.round(maxPriority / shortestPathToSignature));
        });
      });
    },

    isLatest: function(message) {
      return true; // TODO: implement
    },

    saveMessageTrustDistances: function(message) {
      var queries = [];
      if (Message.isPositive(message)) {
        var i, j;
        for (i = 0; i < message.signedData.author.length; i++) {
          for (j = 0; j < message.signedData.recipient.length; j++) {
            queries.push(knex('TrustDistances').insert({
              'start_id_type': message.signedData.author[0],
              'start_id_value': message.signedData.author[1],
              'end_id_type': message.signedData.recipient[0],
              'end_id_value': message.signedData.recipient[1]
            }));
          }
        }
      }

      return P.all(queries);
    }
  };

  return publicMethods;
};
