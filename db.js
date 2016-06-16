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

var SQL_IFNULL = 'IFNULL';
var SQL_INSERT_OR_REPLACE = 'INSERT OR REPLACE';
var SQL_ON_CONFLICT = '';
var SQL_PRINTF = 'PRINTF';

module.exports = function(knex) {
  var p; // Private methods

  var pub = {
    trustIndexedAttributes: null,
    saveMessage: function(message, updateTrustIndexes) {
      if (typeof updateTrustIndexes === 'undefined') { updateTrustIndexes = true; }
      var queries = [];

      var q = this.messageExists(message.hash).then(function(exists) {
          if (!exists) {
            var isPublic = typeof message.signedData.public === 'undefined' ? true : message.signedData.public;
            return p.getPriority(message).then(function(priority) {
              return knex.transaction(function(trx) {
                return trx('Messages').insert({
                  hash:           message.hash,
                  jws:            message.jws,
                  timestamp:      message.signedData.timestamp,
                  type:           message.signedData.type || 'rating',
                  rating:         message.signedData.rating || 0,
                  max_rating:     message.signedData.maxRating || 0,
                  min_rating:     message.signedData.minRating || 0,
                  public:         isPublic,
                  priority:       priority,
                  is_latest:      p.isLatest(message),
                  signer_keyid:   message.signerKeyHash,
                })
                .then(function() {
                  var i, queries = [];
                  for (i = 0; i < message.signedData.author.length; i++) {
                    queries.push(trx('MessageAttributes').insert({
                      message_hash: message.hash,
                      name: message.signedData.author[i][0],
                      value: message.signedData.author[i][1],
                      is_recipient: false
                    }));
                  }
                  for (i = 0; i < message.signedData.recipient.length; i++) {
                    queries.push(trx('MessageAttributes').insert({
                      message_hash: message.hash,
                      name: message.signedData.recipient[i][0],
                      value: message.signedData.recipient[i][1],
                      is_recipient: true
                    }));
                  }
                  return P.all(queries);
                });
              })
              .then(function() {
                if (updateTrustIndexes) {
                  return p.updateWotIndexesByMessage(message)
                  .then(function() {
                    return p.updateIdentityIndexesByMessage(message)
                    .then(function(res) {
                      return res;
                    });
                  });
                }
              });
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
            resolve(!!parseInt(res[0].exists));
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
          options.where['attr.name'] = options.author[0];
          options.where['attr.value'] = options.author[1];
          options.where['attr.is_recipient'] = false;
        } else {
          options.where['attr.name'] = options.recipient[0];
          options.where['attr.value'] = options.recipient[1];
          options.where['attr.is_recipient'] = true;
        }
      }

      var query = knex.select('Messages.*')
        .distinct('Messages.hash')
        .from('Messages')
        .innerJoin('MessageAttributes as attr', 'Messages.hash', 'attr.message_hash')
        .orderBy(options.orderBy, options.direction)
        .limit(options.limit)
        .offset(options.offset);

      if (options.timestampGte) {
        query.andWhere('Messages.timestamp', '>=', options.timestampGte);
      }

      if (options.timestampLte) {
        query.andWhere('Messages.timestamp', '<=', options.timestampLte);
      }

      if (options.where['Messages.type'] && options.where['Messages.type'].match(/^rating:(positive|neutral|negative)$/i)) {
        var ratingType = options.where['Messages.type'].match(/(positive|neutral|negative)$/i)[0];
        options.where['Messages.type'] = 'rating';
        var bindings = { rating: 'Messages.rating', max_rating: 'Messages.max_rating', min_rating: 'Messages.min_rating' };
        switch(ratingType) {
          case 'positive':
            query.whereRaw(':rating: > ( :max_rating: + :min_rating:) / 2', bindings);
            break;
          case 'neutral':
            query.whereRaw(':rating: = ( :max_rating: + :min_rating:) / 2', bindings);
            break;
          case 'negative':
            query.whereRaw(':rating: < ( :max_rating: + :min_rating:) / 2', bindings);
            break;
        }
      }

      query.where(options.where);

      if (options.viewpoint) {
        query.innerJoin('TrustDistances as td', function() {
          this.on('attr.name', '=', 'td.end_attr_name')
            .andOn('attr.value', '=', 'td.end_attr_value')
            .andOn('attr.is_recipient', '=', knex.raw('?', false))
            .andOn('td.start_attr_name', '=', knex.raw('?', options.viewpoint[0]))
            .andOn('td.start_attr_value', '=', knex.raw('?', options.viewpoint[1]));
          if (options.maxDistance > 0) {
            this.andOn('td.distance', '<=', knex.raw('?', options.maxDistance));
          }
        });
      }
      return query;
    },

    dropMessage: function(messageHash) {
      var messageDeleted = 0;
      return knex.transaction(function(trx) {
        return trx('MessageAttributes').where({ message_hash: messageHash }).del()
        .then(function() {
          return trx('Messages').where({ hash: messageHash }).del();
        })
        .then(function(res) {
          return !!res;
        });
      });
    },

    getIdentityAttributes: function(options) {
      var defaultOptions = {
        orderBy: 'value',
        direction: 'asc',
        limit: 100,
        offset: 0,
        where: {},
        having: {},
        viewpoint: myId
      };
      options = options || defaultOptions;
      for (var key in defaultOptions) {
        options[key] = options[key] !== undefined ? options[key] : defaultOptions[key];
      }

      options.where['attr.viewpoint_name'] = options.viewpoint[0];
      options.where['attr.viewpoint_value'] = options.viewpoint[1];

      /* For debugging
      knex('IdentityAttributes').select('*').then(function(res) {
        console.log(res);
      });
      */

      var subquery = knex.from('IdentityAttributes AS attr2')
        .select(knex.raw('attr.identity_id'))
        .groupBy('attr.identity_id')
        .innerJoin('IdentityAttributes AS attr', function() {
          this.on('attr.identity_id', '=', 'attr2.identity_id');
        })
        .leftJoin('TrustDistances as td', function() {
          this.on('td.start_attr_name', '=', 'attr.viewpoint_name');
          this.andOn('td.start_attr_value', '=', 'attr.viewpoint_value');
          this.andOn('td.end_attr_name', '=', 'attr.name');
          this.andOn('td.end_attr_value', '=', 'attr.value');
        })
        .where(options.where)
        .orderBy(knex.raw('MIN(' + SQL_IFNULL + '(td.distance, 100000))'), options.direction)
        .limit(options.limit)
        .offset(options.offset);

      if (options.searchValue) {
        subquery.where('attr.value', 'LIKE', '%' + options.searchValue + '%');
      }

      // subquery.then(function(res) { console.log(JSON.stringify(res, null, 2)); });

      var q = knex.from('IdentityAttributes AS attr')
        .select('*')
        .leftJoin('TrustDistances as td', function() {
          this.on('td.start_attr_name', '=', 'attr.viewpoint_name');
          this.andOn('td.start_attr_value', '=', 'attr.viewpoint_value');
          this.andOn('td.end_attr_name', '=', 'attr.name');
          this.andOn('td.end_attr_value', '=', 'attr.value');
        })
        .where('attr.identity_id', 'in', subquery);

      // sql += "ORDER BY iid >= 0 DESC, IFNULL(tp.Distance,1000) ASC, CASE WHEN attrvalue LIKE :query || '%' THEN 0 ELSE 1 END, UID.name IS NOT NULL DESC, attrvalue ASC ";

      return q.then(function(res) {
        // console.log(JSON.stringify(res, null, 2));
        var arr = [], identities = {};
        for (var i = 0; i < res.length; i++) {
          var attr = res[i];
          identities[attr.identity_id] = identities[attr.identity_id] || [];
          identities[attr.identity_id].push({ attr: attr.name, val: attr.value, dist: attr.distance });
        }

        for (var key in identities) {
            arr.unshift(identities[key]);
        }

        return new P(function(resolve) {
          resolve(arr);
        });
      });

/*
      if (options.searchValue) {
        return knex.from('MessageAttributes').distinct('name', 'value').select()
          .where(options.where)
          .where('value', 'like', '%' + options.searchValue + '%')
          .orderBy(options.orderBy, options.direction)
          .limit(options.limit)
          .offset(options.offset);
      }
      return knex.from('MessageAttributes').distinct('name', 'value').select()
        .where(options.where)
        .orderBy(options.orderBy, options.direction)
        .limit(options.limit)
        .offset(options.offset); */
    },

    mapIdentityAttributes: function(options) {
      var sql = 'identity_id IN (SELECT identity_id FROM "IdentityAttributes" WHERE "name" = ? AND "value" = ? AND "viewpoint_name" = ? AND "viewpoint_value" = ?)';
      var countBefore;

      options.viewpoint = options.viewpoint || myId;

      return knex.transaction(function(trx) {
        return trx('IdentityAttributes')
          .whereRaw(sql, [options.id[0], options.id[1], options.viewpoint[0], options.viewpoint[1]]).del()
          .then(function() {
            return trx('IdentityAttributes').count('* as count');
          })
          .then(function(res) {
            sql = "WITH RECURSIVE transitive_closure(attr1name, attr1val, attr2name, attr2val, distance, path_string, confirmations, refutations) AS ";
            sql += "( ";
            sql += "SELECT attr1.name, attr1.value, attr2.name, attr2.value, 1 AS distance, ";
            sql += SQL_PRINTF + "('%s:%s:%s:%s:',replace(attr1.name,':','::'),replace(attr1.value,':','::'),replace(attr2.name,':','::'),replace(attr2.value,':','::')) AS path_string, ";
            sql += "1 AS Confirmations, "; // TODO fix
            sql += "0 AS Refutations ";
            sql += "FROM \"Messages\" AS p ";
            sql += "INNER JOIN \"TrustDistances\" AS td1 ON ";
            sql += "td1.start_attr_name = :viewpoint_name AND td1.start_attr_value = :viewpoint_value AND ";
            sql += "td1.end_attr_name = 'keyID' AND td1.end_attr_value = p.signer_keyid ";
            sql += "INNER JOIN \"MessageAttributes\" as attr1 ON p.hash = attr1.message_hash ";
            sql += "INNER JOIN \"MessageAttributes\" as attr2 ON p.hash = attr2.message_hash AND attr2.is_recipient = attr1.is_recipient AND (attr1.name != attr2.name OR attr1.value != attr2.value) ";

            // AddMessageFilterSQL(sql, viewpoint, maxDistance, msgType);

            sql += "WHERE attr1.name = :attr AND attr1.value = :val AND p.type IN ('verify_identity','unverify_identity')  ";
            // AddMessageFilterSQLWhere(sql, viewpoint);

            sql += "UNION ALL ";

            sql += "SELECT tc.attr1name, tc.attr1val, attr2.name, attr2.value, tc.distance + 1, ";
            sql += SQL_PRINTF + "('%s%s:%s:',tc.path_string,replace(attr2.name,':','::'),replace(attr2.value,':','::')) AS path_string, ";
            sql += "1 AS Confirmations, "; // TODO fix
            sql += "0 AS Refutations ";
            sql += "FROM \"Messages\" AS p ";
            sql += "JOIN \"TrustDistances\" AS td2 ON ";
            sql += "td2.start_attr_name = :viewpoint_name AND td2.start_attr_value = :viewpoint_value AND ";
            sql += "td2.end_attr_name = 'keyID' AND td2.end_attr_value = p.signer_keyid ";
            sql += "JOIN \"MessageAttributes\" as attr1 ON p.hash = attr1.message_hash AND attr1.is_recipient = :true ";
            sql += "JOIN \"IdentifierAttributes\" AS ia1 ON ia1.name = attr1.name ";
            sql += "JOIN \"MessageAttributes\" as attr2 ON p.hash = attr2.message_hash AND attr2.is_recipient = :true AND (attr1.name != attr2.name OR attr1.value != attr2.value) ";
            sql += "JOIN transitive_closure AS tc ON tc.confirmations > tc.refutations AND attr1.name = tc.attr2name AND attr1.value = tc.attr2val ";
            sql += "INNER JOIN \"IdentifierAttributes\" AS ia2 ON ia2.name = tc.attr1name ";

            // AddMessageFilterSQL(sql, viewpoint, maxDistance, msgType);

            sql += "WHERE tc.distance < 5 AND p.type IN ('verify_identity','unverify_identity') ";
            // AddMessageFilterSQLWhere(sql, viewpoint);
            sql += "AND tc.path_string NOT LIKE " + SQL_PRINTF + "('%%%s:%s:%%',replace(attr2.name,':','::'),replace(attr2.value,':','::'))";
            sql += ") ";

            sql += SQL_INSERT_OR_REPLACE + " INTO \"IdentityAttributes\" ";
            // The subquery for selecting identity_id could be optimized?
            if (SQL_ON_CONFLICT.length) { sql += '('; }
            sql += "SELECT (SELECT " + SQL_IFNULL + "(MAX(identity_id), 0) + 1 FROM \"IdentityAttributes\"), attr2name, attr2val, :viewpoint_name, :viewpoint_value, 1, 0 FROM transitive_closure ";
            sql += "GROUP BY attr2name, attr2val ";
            sql += "UNION SELECT (SELECT " + SQL_IFNULL + "(MAX(identity_id), 0) + 1 FROM \"IdentityAttributes\"), :attr, :val, :viewpoint_name, :viewpoint_value, 1, 0 ";
            sql += "FROM \"MessageAttributes\" AS mi ";
            sql += "INNER JOIN \"IdentifierAttributes\" AS ui ON ui.name = mi.name ";
            sql += "WHERE mi.name = :attr AND mi.value = :val ";
            if (SQL_ON_CONFLICT.length) { sql += ') ' + SQL_ON_CONFLICT; }

            var sqlValues = {
              attr: options.id[0],
              val: options.id[1],
              viewpoint_name: options.viewpoint[0],
              viewpoint_value: options.viewpoint[1],
              true: true
            };

            return trx.raw(sql, sqlValues);
          }).then(function() {
            return trx('IdentityAttributes').count('* as val');
          })
          .then(function(res) {
            if (countBefore === res[0].val) {
              return new P(function(resolve) { resolve([]); });
            }

            var hasSearchedAttributes = options.searchedAttributes && options.searchedAttributes.length > 0;

            if (hasSearchedAttributes) {
              return trx('IdentityAttributes')
                .select('name', 'value', 'confirmations', 'refutations')
                .where(knex.raw('NOT (Name = ? AND value = ?) AND identity_id = (SELECT MAX(identity_id) FROM \"IdentityAttributes\")', [options.id[0], options.id[1]]))
                .whereIn('name', options.searchedAttributes)
                .orderByRaw('confirmations - refutations DESC');
            }

            return trx('IdentityAttributes')
              .select('name', 'value', 'confirmations', 'refutations')
              .where(knex.raw('NOT (Name = ? AND value = ?) AND identity_id = (SELECT MAX(identity_id) FROM \"IdentityAttributes\")', [options.id[0], options.id[1]]))
              .orderByRaw('confirmations - refutations DESC');
          });
      });
    },

    getTrustDistance: function(from, to) {
      if (from[0] === to[0] && from[1] === to[1]) {
        return new P(function(resolve) { resolve(0); });
      }
      return knex.select('distance').from('TrustDistances').where({
        'start_attr_name': from[0],
        'start_attr_value': from[1],
        'end_attr_name': to[0],
        'end_attr_value': to[1]
      }).then(function(res) {
        var distance = -1;
        if (res.length) {
          distance = res[0].distance;
        }
        return new P(function(resolve) { resolve(distance); });
      });
    },

    getTrustDistances: function(from) {
      return knex.select('*').from('TrustDistances').where({
        'start_attr_name': from[0],
        'start_attr_value': from[1]
      });
    },

    getConnectingMessages: function(options) {
      return knex.select('Messages.*').from('Messages')
        .innerJoin('MessageAttributes as attr1', 'Messages.hash', 'attr1.message_hash')
        .innerJoin('MessageAttributes as attr2', 'attr1.message_hash', 'attr2.message_hash')
        .where({
          'attr1.name': options.attr1[0],
          'attr1.value': options.attr1[1],
          'attr1.is_recipient': true,
          'attr2.name': options.attr2[0],
          'attr2.value': options.attr2[1],
          'attr2.is_recipient': true
        });
    },

    /*
      1. build a web of trust consisting of keyIDs only
      2. build a web of trust consisting of all kinds of unique attributes, sourcing from messages
          signed by keyIDs in our web of trust
    */
    generateWebOfTrustIndex: function(id, maxDepth, maintain, trustedKeyID) {
      if (id[0] !== 'keyID' && !trustedKeyID) {
        throw new Error('Please specify a trusted keyID');
      }
      function buildSql(betweenKeyIDsOnly) {
        var sql = "WITH RECURSIVE transitive_closure(attr1name, attr1val, attr2name, attr2val, distance, path_string) AS ";
        sql += "(";
        sql += "SELECT attr1.name, attr1.value, attr2.name, attr2.value, 0 AS distance, ";
        sql += SQL_PRINTF + "('%s:%s:%s:%s:',replace(attr1.name,':','::'),replace(attr1.value,':','::'),replace(attr2.name,':','::'),replace(attr2.value,':','::')) AS path_string ";
        sql += "FROM \"Messages\" AS m ";
        sql += "INNER JOIN \"MessageAttributes\" as attr1 ON m.hash = attr1.message_hash AND attr1.is_recipient = :false ";
        if (betweenKeyIDsOnly) {
          sql += "AND attr1.name = 'keyID' ";
        } else {
          sql += "INNER JOIN \"IdentifierAttributes\" AS uidt1 ON uidt1.name = attr1.name ";
        }
        sql += "INNER JOIN \"MessageAttributes\" as attr2 ON m.hash = attr2.message_hash AND (attr1.name != attr2.name OR attr1.value != attr2.value) ";
        if (betweenKeyIDsOnly) {
          sql += "AND attr2.name = 'keyID' AND attr2.is_recipient = :true ";
        } else {
          sql += "INNER JOIN \"IdentifierAttributes\" AS uidt2 ON uidt2.name = attr2.name ";
          /* Only accept messages whose origin is verified by trusted keyID */
          sql += "LEFT JOIN \"TrustDistances\" AS td ON ";
          sql += "td.start_attr_name = 'keyID' AND td.start_attr_value = :trustedKeyID AND ";
          sql += "td.end_attr_name = 'keyID' AND td.end_attr_value = m.signer_keyid ";
        }
        sql += "WHERE m.is_latest AND m.rating > (m.min_rating + m.max_rating) / 2 AND attr1.name = :attr1name AND attr1.value = :attr1value ";
        if (!betweenKeyIDsOnly) {
          sql += "AND (td.distance IS NOT NULL OR m.signer_keyid = :trustedKeyID) ";
        }

        sql += "UNION ALL ";

        sql += "SELECT tc.attr1name, tc.attr1val, attr2.name, attr2.value, tc.distance + 1, ";
        sql += SQL_PRINTF + "('%s%s:%s:',tc.path_string,replace(attr2.name,':','::'),replace(attr2.value,':','::')) AS path_string ";
        sql += "FROM \"Messages\" AS m ";
        sql += "INNER JOIN \"MessageAttributes\" as attr1 ON m.hash = attr1.message_hash AND attr1.is_recipient = :false ";
        sql += "INNER JOIN \"IdentifierAttributes\" AS uidt1 ON uidt1.name = attr1.name ";
        sql += "INNER JOIN \"MessageAttributes\" as attr2 ON m.hash = attr2.message_hash AND (attr1.name != attr2.name OR attr1.value != attr2.value) ";
        if (betweenKeyIDsOnly) {
          sql += "AND attr2.name = 'keyID' AND attr2.is_recipient = :true ";
        } else {
          sql += "INNER JOIN \"IdentifierAttributes\" AS uidt2 ON uidt2.name = attr2.name ";
          /* Only accept messages whose origin is verified by trusted keyID */
          sql += "LEFT JOIN \"TrustDistances\" AS td ON ";
          sql += "td.start_attr_name = 'keyID' AND td.start_attr_value = :trustedKeyID AND ";
          sql += "td.end_attr_name = 'keyID' AND td.end_attr_value = m.signer_keyid ";
        }
        sql += "JOIN transitive_closure AS tc ON attr1.name = tc.attr2name AND attr1.value = tc.attr2val ";
        sql += "WHERE m.is_latest AND m.rating > (m.min_rating + m.max_rating) / 2 AND tc.distance < :maxDepth AND tc.path_string NOT LIKE " + SQL_PRINTF + "('%%%s:%s:%%',replace(attr2.name,':','::'),replace(attr2.value,':','::')) ";
        if (!betweenKeyIDsOnly) {
          sql += "AND (td.distance IS NOT NULL OR m.signer_keyid = :trustedKeyID) ";
        }
        sql += ") ";
        sql += SQL_INSERT_OR_REPLACE + " INTO \"TrustDistances\" (start_attr_name, start_attr_value, end_attr_name, end_attr_value, distance) SELECT :attr1name, :attr1value, attr2name, attr2val, distance FROM transitive_closure ";
        sql += SQL_ON_CONFLICT;
        return sql;
      }

      var keyIdsSql = buildSql(true),
        allIdsSql = buildSql(false);

      var q;
      if (maintain) {
        q = this.addTrustIndexedAttribute(id, maxDepth);
      } else {
        q = new P(function(resolve) { resolve(); });
      }
      return q.then(function() {
        return knex.transaction(function(trx) {
          return trx('TrustDistances')
            .where({ start_attr_name: id[0], start_attr_value: id[1] }).del()
            .then(function() {
              return trx.raw(keyIdsSql, { attr1name: 'keyID', attr1value: trustedKeyID || id[1], maxDepth: maxDepth, true: true, false: false });
            })
            .then(function() {
              return trx.raw(allIdsSql, { attr1name: id[0], attr1value: id[1], maxDepth: maxDepth, trustedKeyID: trustedKeyID || id[1], true: true, false: false });
            })
            .then(function() {
              return trx('TrustDistances').where({ start_attr_name: id[0], start_attr_value: id[1], end_attr_name: id[0], end_attr_value: id[1], distance: 0 })
              .count('* as count');
            })
            .then(function(res) {
              if (!parseInt(res[0].count)) {
                // Add trust distance to self = 0
                return trx('TrustDistances')
                .insert({ start_attr_name: id[0], start_attr_value: id[1], end_attr_name: id[0], end_attr_value: id[1], distance: 0 });
              }
            })
            .then(function() {
              return trx('TrustDistances').count('* as wot_size')
                .where({ start_attr_name: id[0], start_attr_value: id[1] });
            })
            .then(function(res) {
              return parseInt(res[0].wot_size);
            });
        });
      });
    },

    addTrustIndexedAttribute: function(id, depth) {
      return knex('TrustIndexedAttributes').where({ name: id[0], value: id[1] }).count('* as count')
      .then(function(res) {
        if (parseInt(res[0].count)) {
          return knex('TrustIndexedAttributes').where({ name: id[0], value: id[1] }).update({ depth: depth });
        } else {
          return knex('TrustIndexedAttributes').insert({ name: id[0], value: id[1], depth: depth });
        }
      })
      .then(function() {
        return pub.getTrustIndexedAttributes(true);
      })
      .then(function(res) {
        pub.trustIndexedAttributes = res;
        return knex('TrustDistances')
        .where({ start_attr_name: id[0], start_attr_value: id[1], end_attr_name: id[0], end_attr_value: id[1] })
        .count('* as c');
      })
      .then(function(res) {
        if (parseInt(res[0].c) === 0) {
          // Add trust distance to self = 0
          return knex('TrustDistances')
          .insert({ start_attr_name: id[0], start_attr_value: id[1], end_attr_name: id[0], end_attr_value: id[1], distance: 0 }).return();
        }
      });
    },

    getTrustIndexedAttributes: function(forceRefresh) {
      if (pub.trustIndexedAttributes && !forceRefresh) {
        return new P(function(resolve) { resolve(pub.trustIndexedAttributes); });
      }
      return knex('TrustIndexedAttributes').select('*').then(function(res) {
        pub.trustIndexedAttributes = res;
        return res;
      });
    },

    /*
    identitySearch: function(query, limit, offset, viewpoint) {
      viewpoint = viewpoint || ['', ''];
      var useViewpoint = viewpoint[0] && viewpoint[1];

      var sql = "SELECT IFNULL(OtherAttributes.name,attrname) AS name, IFNULL(OtherAttributes.value,attrvalue) AS value, MAX(iid) AS iid FROM (";
      sql += "SELECT DISTINCT mi.name as attrname, mi.value as attrvalue, -1 AS iid FROM MessageAttributes AS mi ";
      sql += "WHERE ";
      sql += "mi.value LIKE '%' || :query || '%' ";

      if (query[0]) {
        sql += "AND mi.name = :name ";
      }

      sql += "UNION ";
      sql += "SELECT DISTINCT ii.name as attrname, ii.value as attrvalue, ii.identity_id AS iid FROM IdentityAttributes AS ii ";
      sql += "WHERE ";
      sql += "ii.value LIKE '%' || :query || '%' ";

      if (query[0]) {
        sql += "AND ii.name = :name ";
      }

      sql += "AND viewpoint_name = :viewType AND viewpoint_value = :viewID ";
      sql += ") ";

      if (useViewpoint) {
        sql += "LEFT JOIN TrustDistances AS tp ON tp.end_name = attrname AND tp.end_value = attrvalue ";
        sql += "AND tp.start_name = :viewType AND tp.start_value = :viewID ";
      }

      sql += "LEFT JOIN IdentifierAttributes AS UID ON UID.name = attrname ";
      sql += "LEFT JOIN IdentityAttributes AS OtherAttributes ON OtherAttributes.identity_id = iid AND OtherAttributes.confirmations >= OtherAttributes.refutations ";

      if (useViewpoint) {
        sql += "AND OtherAttributes.viewpoint_name = :viewType AND OtherAttributes.viewpoint_value = :viewID ";
      }

      //sql += "LEFT JOIN CachedNames AS cn ON cn.name = name AND cn.value = id ";
      //sql += "LEFT JOIN CachedEmails AS ce ON ce.name = name AND ce.value = id ";

      sql += "GROUP BY IFNULL(OtherAttributes.name,attrname), IFNULL(OtherAttributes.value,attrvalue) ";

      if (useViewpoint) {
        sql += "ORDER BY iid >= 0 DESC, IFNULL(tp.Distance,1000) ASC, CASE WHEN attrvalue LIKE :query || '%' THEN 0 ELSE 1 END, UID.name IS NOT NULL DESC, attrvalue ASC ";
      }

      var params = { query: query[1], name: query[0], viewType: viewpoint[0], viewID: viewpoint[1] };
      return knex.raw(sql, params);
    }, */

    getTrustPaths: function(start, end, maxLength, shortestOnly, viewpoint, limit) {
      if (!viewpoint) {
        if (start[0] === 'keyID') {
          viewpoint = start;
        } else {
          viewpoint = myId;
        }
      }

      limit = limit || 50;

      var sql = '';
      sql += "WITH RECURSIVE transitive_closure(attr1name, attr1val, attr2name, attr2val, distance, path_string) AS ";
      sql += "(";
      sql += "SELECT attr1.name, attr1.value, attr2.name, attr2.value, 1 AS distance, ";
      sql += SQL_PRINTF + "('%s:%s:%s:%s:',replace(attr1.name,':','::'),replace(attr1.value,':','::'),replace(attr2.name,':','::'),replace(attr2.value,':','::')) AS path_string ";
      sql += "FROM \"Messages\" AS m ";
      sql += "INNER JOIN \"TrustDistances\" as td ON ";
      sql += "td.start_attr_name = :viewpoint_name AND td.start_attr_value = :viewpoint_value AND ";
      sql += "td.end_attr_name = 'keyID' AND td.end_attr_value = m.signer_keyid ";
      sql += "INNER JOIN \"MessageAttributes\" as attr1 ON m.Hash = attr1.message_hash AND attr1.is_recipient = :false ";
      sql += "INNER JOIN \"IdentifierAttributes\" AS ia1 ON ia1.name = attr1.name ";
      sql += "INNER JOIN \"MessageAttributes\" as attr2 ON m.Hash = attr2.message_hash AND (attr1.name != attr2.name OR attr1.value != attr2.value) ";
      sql += "INNER JOIN \"IdentifierAttributes\" AS ia2 ON ia2.name = attr2.name ";
      sql += "WHERE m.is_latest AND m.Rating > (m.min_rating + m.max_rating) / 2 AND attr1.name = :attr1name AND attr1.value = :attr1val ";

      sql += "UNION ALL ";

      sql += "SELECT tc.attr1name, tc.attr1val, attr2.name, attr2.value, tc.distance + 1, ";
      sql += SQL_PRINTF + "('%s%s:%s:',tc.path_string,replace(attr2.name,':','::'),replace(attr2.value,':','::')) AS path_string ";
      sql += "FROM \"Messages\" AS m ";
      sql += "INNER JOIN \"TrustDistances\" as td ON ";
      sql += "td.start_attr_name = :viewpoint_name AND td.start_attr_value = :viewpoint_value AND ";
      sql += "td.end_attr_name = 'keyID' AND td.end_attr_value = m.signer_keyid ";
      sql += "INNER JOIN \"MessageAttributes\" as attr1 ON m.Hash = attr1.message_hash AND attr1.is_recipient = :false ";
      sql += "INNER JOIN \"IdentifierAttributes\" AS ia1 ON ia1.name = attr1.name ";
      sql += "INNER JOIN \"MessageAttributes\" as attr2 ON m.Hash = attr2.message_hash AND (attr1.name != attr2.name OR attr1.value != attr2.value) ";
      sql += "INNER JOIN \"IdentifierAttributes\" AS ia2 ON ia2.name = attr2.name ";
      sql += "JOIN transitive_closure AS tc ON attr1.name = tc.attr2name AND attr1.value = tc.attr2val ";
      sql += "WHERE m.is_latest AND m.Rating > (m.min_rating + m.max_rating) / 2 AND tc.distance < :max_length AND tc.path_string NOT LIKE " + SQL_PRINTF + "('%%%s:%s:%%',replace(attr2.name,':','::'),replace(attr2.value,':','::')) ";
      sql += ") ";
      sql += "SELECT path_string FROM transitive_closure ";
      sql += "WHERE attr2name = :attr2name AND attr2val = :attr2val ";
      sql += "ORDER BY distance ";
      sql += "LIMIT :limit ";

      return knex.raw(sql,
        { attr1name: start[0],
          attr1val: start[1],
          max_length: maxLength,
          attr2name: end[0],
          attr2val: end[1],
          viewpoint_name: viewpoint[0],
          viewpoint_value: viewpoint[1],
          limit: limit,
          true: true,
          false: false
        })
        .then(function(res) {
          return res.rows || res;
        });
    },

    getMessageCount: function() {
      return knex('Messages').count('* as val')
      .then(function(res) {
        return parseInt(res[0].val);
      });
    },

    getStats: function(id, options) {
      var sql = "";
      sql += "SUM(CASE WHEN attr.is_recipient = :false AND m.rating > (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS sent_positive, ";
      sql += "SUM(CASE WHEN attr.is_recipient = :false AND m.rating = (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS sent_neutral, ";
      sql += "SUM(CASE WHEN attr.is_recipient = :false AND m.rating < (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS sent_negative, ";
      if (options.viewpoint) {
        sql += "SUM(CASE WHEN attr.is_recipient = :true AND m.rating > (m.min_rating + m.max_rating) / 2 AND ";
        sql += "(td.start_attr_value IS NOT NULL) THEN 1 ELSE 0 END) AS received_positive, ";
        sql += "SUM(CASE WHEN attr.is_recipient = :true AND m.rating = (m.min_rating + m.max_rating) / 2 AND ";
        sql += "(td.start_attr_value IS NOT NULL) THEN 1 ELSE 0 END) AS received_neutral, ";
        sql += "SUM(CASE WHEN attr.is_recipient = :true AND m.rating < (m.min_rating + m.max_rating) / 2 AND  ";
        sql += "(td.start_attr_value IS NOT NULL) THEN 1 ELSE 0 END) AS received_negative, ";
      } else {
        sql += "SUM(CASE WHEN attr.is_recipient = :true AND m.rating > (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS received_positive, ";
        sql += "SUM(CASE WHEN attr.is_recipient = :true AND m.rating = (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS received_neutral, ";
        sql += "SUM(CASE WHEN attr.is_recipient = :true AND m.rating < (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS received_negative, ";
      }
      sql += "MIN(m.timestamp) AS first_seen ";

      var query = knex('Messages as m')
        .innerJoin('MessageAttributes as attr', 'attr.message_hash', 'm.hash')
        .innerJoin('IdentifierAttributes as ia', 'ia.name', 'attr.name')
        .select(knex.raw(sql, {
          viewpoint_name: options.viewpoint ? options.viewpoint[0] : null,
          viewpointID: options.viewpoint ? options.viewpoint[1] : null,
          true: true,
          false: false
        }))
        .where({ 'm.is_latest': 1, 'm.type': 'rating', 'attr.name': id[0], 'attr.value': id[1] });

      if (options.viewpoint) {
        query.leftJoin('TrustDistances as td', function() {
          this.on('attr.name', '=', 'td.end_attr_name')
            .andOn('attr.value', '=', 'td.end_attr_value')
            .andOn('attr.is_recipient', '=', knex.raw('?', false));
        });

        sql = '(td.start_attr_name = :viewpoint_name AND td.start_attr_value = :viewpoint_value ';
        if (options.maxDistance > 0) {
          sql += 'AND td.distance <= :maxDistance ';
        }
        // A bit messy way to pick also messages that were authored by the viewpointId
        sql += ') OR (attr.is_recipient = :false AND attr.name = :viewpoint_name AND attr.value = :viewpoint_value)';
        query.where(knex.raw(sql, {
          viewpoint_name: options.viewpoint[0],
          viewpoint_value: options.viewpoint[1],
          maxDistance: options.maxDistance,
          true: true,
          false: false
        }));

        var subquery = knex('IdentityAttributes').where({
          viewpoint_name: options.viewpoint[0],
          viewpoint_value: options.viewpoint[1],
          name: id[0],
          value: id[1]
        }).select('identity_id');

        query.leftJoin('IdentityAttributes AS i', function() {
          this.on('attr.name', '=', 'i.name')
            .andOn('attr.value', '=', 'i.value');
        })
        .whereIn('i.identity_id', subquery)
        .groupBy('i.identity_id', 'm.hash');
      }

      return query.then(function(res) {
        for (var key in res[0]) {
          if (key.indexOf('sent_') === 0 || key.indexOf('received_') === 0) {
            res[0][key] = parseInt(res[0][key]);
          }
        }
        return res;
      });
    },

    updatePeerLastSeen: function(peer) {
      return knex('Peers').where({ url: peer.url }).update({ last_seen: peer.last_seen || null });
    },

    addPeer: function(peer) {
      var _ = this;
      return knex('Peers').where({ url: peer.url }).count('* as count')
      .then(function(res) {
        if (parseInt(res[0].count) === 0) {
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
      return knex('Peers').count('* as count').then(function(res) { return parseInt(res.count); });
    },

    checkDefaultTrustList: function(db) {
      var _ = this;
      return knex('Messages').count('* as count')
      .then(function(res) {
        if (parseInt(res[0].count) === 0) {
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
          if (res > config.maxMessageCount) {
            var nMessagesToDelete = Math.min(100, Math.ceil(config.maxMessageCount / 10));
            var messagesToDelete = knex('Messages').select('hash').limit(nMessagesToDelete).orderBy('priority', 'asc').orderBy('created', 'asc');
            return knex('Messages').whereIn('hash', messagesToDelete).del()
              .then(function(res) {
                return knex('MessageAttributes').whereIn('message_hash', messagesToDelete).del();
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

      Messages authored or received by attributes of type keyID have slightly
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
        'start_attr_name': startId[0],
        'start_attr_value': startId[1],
        'end_attr_name': endId[0],
        'end_attr_value': endId[1]
      }).del()
      .then(function() {
        return knex('TrustDistances').insert({
          'start_attr_name': startId[0],
          'start_attr_value': startId[1],
          'end_attr_name': endId[0],
          'end_attr_value': endId[1],
          'distance': distance
        });
      });
    },

    saveIdentityAttribute: function(identifier, viewpoint, identityId, confirmations, refutations) {
      return knex('IdentityAttributes')
        .insert({
          viewpoint_name: viewpoint[0],
          viewpoint_value: viewpoint[1],
          name: identifier[0],
          value: identifier[1],
          identity_id: identityId,
          confirmations: confirmations,
          refutations: refutations
        });
    },

    getIdentityID: function(identifier, viewpoint) {
      return knex
        .from('IdentityAttributes')
        .where({
          viewpoint_name: viewpoint[0],
          viewpoint_value: viewpoint[1],
          name: identifier[0],
          value: identifier[1]
        })
        .innerJoin('IdentifierAttributes', 'IdentityAttributes.name', 'IdentifierAttributes.name')
        .select('IdentityAttributes.identity_id');
    },

    updateIdentityIndexesByMessage: function(message, trustedKeyID) {
      if (message.signedData.type !== 'verify_identity' && message.signedData.type !== 'unverify_identity') {
        return new P(function(resolve) {
          resolve();
        });
      }

      var queries = [];

      // TODO: make this faster
      return pub.getTrustIndexedAttributes().then(function(viewpoints) {
        for (var j = 0; j < viewpoints.length; j++) {
          var viewpoint = [viewpoints[j].name, viewpoints[j].value];
          for (var i = 0; i < message.signedData.recipient.length; i++) {
            queries.push(
              pub.mapIdentityAttributes({ id: message.signedData.recipient[i], viewpoint: viewpoint })
            );
          }
        }
        return P.all(queries);
      });

      // TODO:
      // Get TrustIndexedAttributes as t
      // Return unless message signer and author are trusted by t
      // Find existing or new identity_id for message recipient IdentifierAttributes
      // If the attribute exists on the identity_id, increase confirmations or refutations
      // If the attribute doesn't exist, add it with 1 confirmation or refutation
    },

    updateWotIndexesByMessage: function(message, trustedKeyID) {
      var queries = [];

      function makeSubquery(a, r) {
        return knex
        .from('TrustIndexedAttributes AS viewpoint')
        .innerJoin('IdentifierAttributes as ia', 'ia.name', knex.raw('?', r[0]))
        .leftJoin('TrustDistances AS td', function() {
          this.on('td.start_attr_name', '=', 'viewpoint.name')
          .andOn('td.start_attr_value', '=', 'viewpoint.value')
          .andOn('td.end_attr_name', '=', knex.raw('?', a[0]))
          .andOn('td.end_attr_value', '=', knex.raw('?', a[1]));
        })
        .leftJoin('TrustDistances AS existing', function() { // TODO: fix with REPLACE or sth. Should update if new distance is shorter.
          this.on('existing.start_attr_name', '=', 'viewpoint.name')
          .andOn('existing.start_attr_value', '=', 'viewpoint.value')
          .andOn('existing.end_attr_name', '=', knex.raw('?', r[0]))
          .andOn('existing.end_attr_value', '=', knex.raw('?', r[1]));
        })
        .whereRaw('existing.distance IS NULL AND ((viewpoint.name = :author_name AND viewpoint.value = :author_value) ' +
          'OR (td.end_attr_name = :author_name AND td.end_attr_value = :author_value))',
          { author_name: a[0], author_value: a[1] })
        .select('viewpoint.name as start_attr_name', 'viewpoint.value as start_attr_value', knex.raw('? as end_attr_name', r[0]), knex.raw('? as end_attr_value', r[1]), knex.raw(SQL_IFNULL+'(td.distance, 0) + 1 as distance'));
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
    if (conf.db.client === 'pg') {
      SQL_IFNULL = 'COALESCE';
      SQL_INSERT_OR_REPLACE = 'INSERT';
      SQL_ON_CONFLICT = 'ON CONFLICT DO NOTHING ';
      SQL_PRINTF = 'format';
    }
    return schema.init(knex, config)
      .then(function() {
        // TODO: if myId is changed, the old one should be removed from TrustIndexedAttributes
        return pub.addTrustIndexedAttribute(myId, myTrustIndexDepth);
      })
      .then(function() {
        return pub.checkDefaultTrustList();
      });
  };

  return pub;
};
