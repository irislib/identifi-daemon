/*jshint unused: false */
'use strict';
var schema = require('./schema.js');
var P = require("bluebird");
var moment = require('moment');

var Message = require('identifi-lib/message');
var keyutil = require('identifi-lib/keyutil');
var myKey = keyutil.getDefault();
var myId = ['keyID', myKey.hash];
var myTrustIndexDepth = 4;
var config;

module.exports = function(knex) {
  var p; // Private methods

  var pub = {
    saveMessage: function(message, updateTrustIndexes) {
      if (typeof updateTrustIndexes === 'undefined') { updateTrustIndexes = true; }
      var queries = [];

      var q = this.messageExists(message.hash).then(function(exists) {
          if (!exists) {
            var isPublic = typeof message.signedData.public === 'undefined' ? true : message.signedData.public;
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
                public:         isPublic,
                priority:       priority,
                is_latest:      p.isLatest(message),
                signer_keyid:   message.signerKeyHash,
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
              if (updateTrustIndexes) {
                queries.push(p.updateWotIndexesByMessage(message));
              }
              return P.all(queries);
            });
          } else {
            return new P(function(resolve) {
              resolve(false);
            });
          }
        });

        return this.ensureFreeSpace().then(function() {
          return q;
        });
    },

    messageExists: function(hash) {
      return knex('Messages').where('hash', hash).count('* as exists')
        .then(function(res) {
          return new P(function(resolve) {
            resolve(!!res[0].exists);
          });
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
      }

      var query = knex.select('Messages.*').from('Messages')
        .innerJoin('MessageIdentifiers as id', 'Messages.hash', 'id.message_hash')
        .where(options.where)
        .orderBy(options.orderBy, options.direction)
        .limit(options.limit)
        .offset(options.offset)
        .groupBy('Messages.hash');

      if (options.timestampGte) {
        query.andWhere('Messages.timestamp', '>=', options.timestampGte);
      }

      if (options.timestampLte) {
        query.andWhere('Messages.timestamp', '<=', options.timestampLte);
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
        query.where(ratingTypeFilter[0], ratingTypeFilter[1], ratingTypeFilter[2]);
      }

      if (options.viewpoint) {
        query.leftJoin('TrustDistances as td', function() {
          this.on('id.type', '=', 'td.end_id_type')
            .andOn('id.value', '=', 'td.end_id_value')
            .andOn('id.is_recipient', '=', '0');
        });

        var sql = '(td.start_id_type = :viewpointType AND td.start_id_value = :viewpointValue ';
        if (options.maxDistance > 0) {
          sql += 'AND td.distance <= :maxDistance ';
        }
        // A bit messy way to pick also messages that were authored by the viewpointId
        sql += ') OR (id.is_recipient = 0 AND id.type = :viewpointType AND id.value = :viewpointValue)';
        query.where(knex.raw(sql, {
          viewpointType: options.viewpoint[0],
          viewpointValue: options.viewpoint[1],
          maxDistance: options.maxDistance
        }));
      }

      return query;
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
      options.viewpoint = options.viewpoint || ['', ''];

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
          sql += "SELECT " + identityID + ", id2type, id2val, :viewpointType, :viewpointID, 1, 0 FROM transitive_closure ";
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

    getTrustDistance: function(from, to) {
      if (from[0] === to[0] && from[1] === to[1]) {
        return new P(function(resolve) { resolve(0); });
      }
      return knex.select('distance').from('TrustDistances').where({
        'start_id_type': from[0],
        'start_id_value': from[1],
        'end_id_type': to[0],
        'end_id_value': to[1]
      }).then(function(res) {
        var distance = -1;
        if (res.length) {
          distance = res[0].distance;
        }
        return new P(function(resolve) { resolve(distance); });
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

    /*
      1. build a web of trust consisting of keyIDs only
      2. build a web of trust consisting of all kinds of unique identifiers, sourcing from messages
          signed by keyIDs in our web of trust
    */
    generateWebOfTrustIndex: function(id, maxDepth, maintain, trustedKeyID) {
      if (id[0] !== 'keyID' && !trustedKeyID) {
        throw new Error('Please specify a trusted keyID');
      }
      function buildSql(betweenKeyIDsOnly) {
        var sql = "WITH RECURSIVE transitive_closure(id1type, id1val, id2type, id2val, distance, path_string) AS ";
        sql += "(";
        sql += "SELECT id1.type, id1.value, id2.type, id2.value, 1 AS distance, ";
        sql += "printf('%s:%s:%s:%s:',replace(id1.type,':','::'),replace(id1.value,':','::'),replace(id2.type,':','::'),replace(id2.value,':','::')) AS path_string ";
        sql += "FROM Messages AS m ";
        sql += "INNER JOIN MessageIdentifiers AS id1 ON m.hash = id1.message_hash AND id1.is_recipient = 0 ";
        if (betweenKeyIDsOnly) {
          sql += "AND id1.type = 'keyID' ";
        } else {
          sql += "INNER JOIN UniqueIdentifierTypes AS uidt1 ON uidt1.type = id1.type ";
        }
        sql += "INNER JOIN MessageIdentifiers AS id2 ON m.hash = id2.message_hash AND (id1.type != id2.type OR id1.value != id2.value) ";
        if (betweenKeyIDsOnly) {
          sql += "AND id2.type = 'keyID' AND id2.is_recipient = 1 ";
        } else {
          sql += "INNER JOIN UniqueIdentifierTypes AS uidt2 ON uidt2.type = id2.type ";
          /* Only accept messages whose origin is verified by trusted keyID */
          sql += "LEFT JOIN TrustDistances AS td ON ";
          sql += "td.start_id_type = 'keyID' AND td.start_id_value = :trustedKeyID AND ";
          sql += "td.end_id_type = 'keyID' AND td.end_id_value = m.signer_keyid ";
        }
        sql += "WHERE m.is_latest AND m.rating > (m.min_rating + m.max_rating) / 2 AND id1.type = :id1type AND id1.value = :id1value ";
        if (!betweenKeyIDsOnly) {
          sql += "AND (td.distance IS NOT NULL OR m.signer_keyid = :trustedKeyID) ";
        }

        sql += "UNION ALL ";

        sql += "SELECT tc.id1type, tc.id1val, id2.type, id2.value, tc.distance + 1, ";
        sql += "printf('%s%s:%s:',tc.path_string,replace(id2.type,':','::'),replace(id2.value,':','::')) AS path_string ";
        sql += "FROM Messages AS m ";
        sql += "INNER JOIN MessageIdentifiers AS id1 ON m.hash = id1.message_hash AND id1.is_recipient = 0 ";
        sql += "INNER JOIN UniqueIdentifierTypes AS uidt1 ON uidt1.type = id1.type ";
        sql += "INNER JOIN MessageIdentifiers AS id2 ON m.hash = id2.message_hash AND (id1.type != id2.type OR id1.value != id2.value) ";
        if (betweenKeyIDsOnly) {
          sql += "AND id2.type = 'keyID' AND id2.is_recipient = 1 ";
        } else {
          sql += "INNER JOIN UniqueIdentifierTypes AS uidt2 ON uidt2.type = id2.type ";
          /* Only accept messages whose origin is verified by trusted keyID */
          sql += "LEFT JOIN TrustDistances AS td ON ";
          sql += "td.start_id_type = 'keyID' AND td.start_id_value = :trustedKeyID AND ";
          sql += "td.end_id_type = 'keyID' AND td.end_id_value = m.signer_keyid ";
        }
        sql += "JOIN transitive_closure AS tc ON id1.type = tc.id2type AND id1.value = tc.id2val ";
        sql += "WHERE m.is_latest AND m.rating > (m.min_rating + m.max_rating) / 2 AND tc.distance < :maxDepth AND tc.path_string NOT LIKE printf('%%%s:%s:%%',replace(id2.type,':','::'),replace(id2.value,':','::')) ";
        if (!betweenKeyIDsOnly) {
          sql += "AND (td.distance IS NOT NULL OR m.signer_keyid = :trustedKeyID) ";
        }
        sql += ") ";
        sql += "INSERT OR REPLACE INTO TrustDistances (start_id_type, start_id_value, end_id_type, end_id_value, distance) SELECT :id1type, :id1value, id2type, id2val, distance FROM transitive_closure ";
        return sql;
      }

      var keyIdsSql = buildSql(true),
        allIdsSql = buildSql(false);

      if (maintain) {
        this.addTrustIndexedIdentifier(id, maxDepth).return();
      }
      return knex('TrustDistances')
        .where({ start_id_type: id[0], start_id_value: id[1] }).del()
        .then(function() {
          return knex.raw(keyIdsSql, { id1type: 'keyID', id1value: trustedKeyID || id[1], maxDepth: maxDepth });
        })
        .then(function() {
          return knex.raw(allIdsSql, { id1type: id[0], id1value: id[1], maxDepth: maxDepth, trustedKeyID: trustedKeyID || id[1] });
        })
        .then(function() {
          return knex('TrustDistances').count('* as wot_size')
            .where({ start_id_type: id[0], start_id_value: id[1] });
        });
    },

    addTrustIndexedIdentifier: function(id, depth) {
      return knex('TrustIndexedIdentifiers').where({ type: id[0], value: id[1] }).count('* as count')
      .then(function(res) {
        if (res[0].count) {
          return knex('TrustIndexedIdentifiers').where({ type: id[0], value: id[1] }).update({ depth: depth });
        } else {
          return knex('TrustIndexedIdentifiers').insert({ type: id[0], value: id[1], depth: depth });
        }
      });
    },

    getWebOfTrustIndexes: function() {
      return knex('TrustIndexedIdentifiers').select('*');
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

    getStats: function(id, options) {
      var sql = "";
      sql += "SUM(CASE WHEN id.is_recipient = 0 AND m.rating > (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS sentPositive, ";
      sql += "SUM(CASE WHEN id.is_recipient = 0 AND m.rating == (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS sentNeutral, ";
      sql += "SUM(CASE WHEN id.is_recipient = 0 AND m.rating < (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS sentNegative, ";
      if (options.viewpoint) {
        sql += "SUM(CASE WHEN id.is_recipient = 1 AND m.rating > (m.min_rating + m.max_rating) / 2 AND ";
        sql += "(td.start_id_value IS NOT NULL) THEN 1 ELSE 0 END) AS receivedPositive, ";
        sql += "SUM(CASE WHEN id.is_recipient = 1 AND m.rating == (m.min_rating + m.max_rating) / 2 AND ";
        sql += "(td.start_id_value IS NOT NULL) THEN 1 ELSE 0 END) AS receivedNeutral, ";
        sql += "SUM(CASE WHEN id.is_recipient = 1 AND m.rating < (m.min_rating + m.max_rating) / 2 AND  ";
        sql += "(td.start_id_value IS NOT NULL) THEN 1 ELSE 0 END) AS receivedNegative, ";
      } else {
        sql += "SUM(CASE WHEN id.is_recipient = 1 AND m.rating > (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS receivedPositive, ";
        sql += "SUM(CASE WHEN id.is_recipient = 1 AND m.rating == (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS receivedNeutral, ";
        sql += "SUM(CASE WHEN id.is_recipient = 1 AND m.rating < (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS receivedNegative, ";
      }
      sql += "MIN(m.timestamp) AS firstSeen ";

      var query = knex('Messages as m')
        .innerJoin('MessageIdentifiers AS id', 'id.message_hash', 'm.hash')
        .innerJoin('UniqueIdentifierTypes as uit', 'uit.type', 'id.type')
        .select(knex.raw(sql, {
          viewpointType: options.viewpoint ? options.viewpoint[0] : null,
          viewpointID: options.viewpoint ? options.viewpoint[1] : null
        }))
        .where({ 'm.is_latest': 1, 'm.type': 'rating', 'id.type': id[0], 'id.value': id[1] });

      if (options.viewpoint) {
        query.leftJoin('TrustDistances as td', function() {
          this.on('id.type', '=', 'td.end_id_type')
            .andOn('id.value', '=', 'td.end_id_value')
            .andOn('id.is_recipient', '=', '0');
        });

        sql = '(td.start_id_type = :viewpointType AND td.start_id_value = :viewpointValue ';
        if (options.maxDistance > 0) {
          sql += 'AND td.distance <= :maxDistance ';
        }
        // A bit messy way to pick also messages that were authored by the viewpointId
        sql += ') OR (id.is_recipient = 0 AND id.type = :viewpointType AND id.value = :viewpointValue)';
        query.where(knex.raw(sql, {
          viewpointType: options.viewpoint[0],
          viewpointValue: options.viewpoint[1],
          maxDistance: options.maxDistance
        }));

        var subquery = knex('Identities').where({
          viewpoint_type: options.viewpoint[0],
          viewpoint_value: options.viewpoint[1],
          type: id[0],
          value: id[1]
        }).select('identity_id');

        query.leftJoin('Identities AS i', function() {
          this.on('id.type', '=', 'i.type')
            .andOn('id.value', '=', 'i.value');
        })
        .whereIn('i.identity_id', subquery)
        .groupBy('i.identity_id', 'm.hash');
      }

      return query;
    },

    updatePeerLastSeen: function(peer) {
      return knex('Peers').where({ url: peer.url }).update({ last_seen: peer.last_seen || null });
    },

    addPeer: function(peer) {
      var _ = this;
      return knex('Peers').where({ url: peer.url }).count('* as count')
      .then(function(res) {
        if (res[0].count === 0) {
          return knex('Peers').insert(peer);
        } else {
          return _.updatePeerLastSeen(peer);
        }
      });
    },

    getPeers: function(where) {
      where = where || {};
      return knex('Peers').select('url', 'last_seen').where(where).orderBy('last_seen', 'desc');
    },

    getPeerCount: function() {
      return knex('Peers').count('* as count');
    },

    checkDefaultTrustList: function(db) {
      var _ = this;
      return knex('Messages').count('* as count')
      .then(function(res) {
        if (res[0].count === 0) {
          var queries = [];
          var message = Message.createRating({
            author: [myId],
            recipient: [['keyID', 'NK0R68KzRFFOZq8mHsyu7GL1jtJXS7LFdATPyXkMBb0=']],
            comment: 'An Identifi seed node, trusted by default',
            rating: 10,
            context: 'identifi_network',
            public: false
          });
          Message.sign(message, myKey.private.pem, myKey.public.hex);
          queries.push(_.saveMessage(message));
          return P.all(queries);
        }
      });
    },

    ensureFreeSpace: function() {
      return this.getMessageCount()
        .then(function(res) {
          if (res[0].val > config.maxMessageCount) {
            var nMessagesToDelete = Math.min(100, Math.ceil(config.maxMessageCount / 10));
            var messagesToDelete = knex('Messages').select('hash').limit(nMessagesToDelete).orderBy('priority', 'asc').orderBy('created', 'asc');
            return knex('Messages').whereIn('hash', messagesToDelete).del()
              .then(function(res) {
                return knex('MessageIdentifiers').whereIn('message_hash', messagesToDelete).del();
              });
          }
        });
    }
  };

  p = {
    /*
      Message priority algorithm based on
      1. trust distance of signer from our key
      2. trust distance of message author from our key.
      In addition to trust distance, the amount and strength of positive and
      negative ratings and their distance could be taken into account.

      Messages authored or received by identifiers of type keyID have slightly
      higher priority.
    */
    getPriority: function(message) {
      var maxPriority = 100;
      var keyType = 'keyID';
      var priority;

      message.signerKeyHash = Message.getSignerKeyHash(message);

      return pub.getTrustDistance(myId, ['keyID', message.signerKeyHash])
      .then(function(distanceToSigner) {
        if (distanceToSigner === -1) { // Unknown signer
          return new P(function(resolve) { resolve(0); });
        }
        var i, queries = [];
        // Get distances to message authors
        for (i = 0; i < message.signedData.author.length; i++) {
          var q = pub.getTrustDistance(myId, message.signedData.author[i]);
          queries.push(q);
        }

        return P.all(queries).then(function(authorDistances) {
          var shortestDistanceToAuthor = 10000000;
          for (var j = 0; j < authorDistances.length; j++) {
            if (authorDistances[j] > -1 && authorDistances[j] < shortestDistanceToAuthor) {
              shortestDistanceToAuthor = authorDistances[j];
            }
          }

          priority = maxPriority / (distanceToSigner + 1);
          priority = priority / 2 + priority / (shortestDistanceToAuthor + 2);
          priority = Math.round(priority);

          var hasAuthorKeyID, hasRecipientKeyID, i;
          for (i = 0; i < message.signedData.author.length; i++) {
            if (message.signedData.author[i][0] === 'keyID') {
              hasAuthorKeyID = true;
              break;
            }
          }
          for (i = 0; i < message.signedData.recipient.length; i++) {
            if (message.signedData.recipient[i][0] === 'keyID') {
              hasRecipientKeyID = true;
              break;
            }
          }
          if (!hasAuthorKeyID) { priority -= 1; }
          if (!hasRecipientKeyID) { priority -= 1; }
          priority = Math.max(priority, 0);
          return new P(function(resolve) { resolve(priority); });
        });
      });
    },

    isLatest: function(message) {
      return true; // TODO: implement
    },

    saveTrustDistance: function(startId, endId, distance) {
      return knex('TrustDistances').where({
        'start_id_type': startId[0],
        'start_id_value': startId[1],
        'end_id_type': endId[0],
        'end_id_value': endId[1]
      }).del()
      .then(function() {
        return knex('TrustDistances').insert({
          'start_id_type': startId[0],
          'start_id_value': startId[1],
          'end_id_type': endId[0],
          'end_id_value': endId[1],
          'distance': distance
        });
      });
    },

    updateWotIndexesByMessage: function(message, trustedKeyID) {
      var queries = [];

      function makeSubquery(a, r) {
        return knex
        .from('TrustIndexedIdentifiers AS viewpoint')
        .innerJoin('UniqueIdentifierTypes as uit', 'uit.type', knex.raw('?', r[0]))
        .leftJoin('TrustDistances AS td', function() {
          this.on('td.start_id_type', '=', 'viewpoint.type')
          .andOn('td.start_id_value', '=', 'viewpoint.value')
          .andOn('td.end_id_type', '=', knex.raw('?', a[0]))
          .andOn('td.end_id_value', '=', knex.raw('?', a[1]));
        })
        .leftJoin('TrustDistances AS existing', function() { // TODO: fix with REPLACE or sth. Should update if new distance is shorter.
          this.on('existing.start_id_type', '=', 'viewpoint.type')
          .andOn('existing.start_id_value', '=', 'viewpoint.value')
          .andOn('existing.end_id_type', '=', knex.raw('?', r[0]))
          .andOn('existing.end_id_value', '=', knex.raw('?', r[1]));
        })
        .whereRaw('existing.distance IS NULL AND ((viewpoint.type = :author_type AND viewpoint.value = :author_value) ' +
          'OR (td.end_id_type = :author_type AND td.end_id_value = :author_value))',
          { author_type: a[0], author_value: a[1] })
        .select('viewpoint.type as start_id_type', 'viewpoint.value as start_id_value', knex.raw('? as end_id_type', r[0]), knex.raw('? as end_id_value', r[1]), knex.raw('IFNULL(td.distance, 0) + 1 as distance'));
      }

      function getSaveFunction(a, r) {
        return function(distance) {
          if (distance > -1) {
            return knex('TrustDistances').insert(makeSubquery(a, r));
          }
        };
      }

      if (Message.isPositive(message)) {
        var i, j;
        for (i = 0; i < message.signedData.author.length; i++) {
          var a = message.signedData.author[i];
          for (j = 0; j < message.signedData.recipient.length; j++) {
            var t = a[0] === 'keyID' ? a[1] : trustedKeyID;
            if (!t) { continue; }
            var r = message.signedData.recipient[j];
            var q = pub.getTrustDistance(['keyID', t], ['keyID', message.signerKeyHash])
            .then(getSaveFunction(a, r));
            queries.push(q);
          }
        }
      }

      return P.all(queries);
    }
  };

  pub.init = function(conf) {
    config = conf;
    return schema.init(knex, config)
      .then(function() {
        // TODO: if myId is changed, the old one should be removed from TrustIndexedIdentifiers
        return pub.addTrustIndexedIdentifier(myId, myTrustIndexDepth);
      })
      .then(function() {
        return pub.checkDefaultTrustList();
      });
  };

  return pub;
};
