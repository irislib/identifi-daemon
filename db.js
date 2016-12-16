/*jshint unused: false */
'use strict';
var util = require('./util.js');
var schema = require('./schema.js');
var P = require("bluebird");
var Promise = P; // For ipfs

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
      if (typeof message.signerKeyHash === 'undefined') {
        message.signerKeyHash = Message.getSignerKeyHash(message);
      }
      var queries = [];

      var q = new P(function(resolve) { resolve(); });
      // Unobtrusively store msg to ipfs
      var addToIpfs = p.ipfs && !message.ipfs_hash;
      if (addToIpfs) {
        q = pub.addMessageToIpfs(message)
        .then(function(res) {
          message.ipfs_hash = res[0].hash;
        }).catch(function(e) { console.log('adding to ipfs failed:', e); });
      }
      q = q.then(function() {
        return pub.messageExists(message.hash);
      })
      .then(function(exists) {
        if (exists) {
          if (addToIpfs && message.ipfs_hash) {
            // Msg was added to IPFS - update ipfs_hash
            return knex('Messages').where({ hash: message.hash }).update({ ipfs_hash: message.ipfs_hash });
          } else {
            return new P(function(resolve) {
              resolve(false);
            });
          }
        } else {
          var isPublic = typeof message.signedData.public === 'undefined' ? true : message.signedData.public;
          return p.deletePreviousMessage(message)
          .then(function() {
            return p.getPriority(message);
          })
          .then(function(priority) {
            return knex.transaction(function(trx) {
              return trx('Messages').insert({
                hash:           message.hash,
                ipfs_hash:      message.ipfs_hash,
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
          })
          .then(function() {
            return message;
          });
        }
      });

      return this.ensureFreeSpace().then(function() {
        return q;
      });
    },

    addMessageToIpfs: function(message) {
      return p.ipfs.files.add(new Buffer(message.jws, 'utf8'));
    },

    addMessagesToIpfs: function() {
      var counter = 0;
      var hash;

      function getReindexQuery() {
        return knex('Messages').whereNull('ipfs_hash').select('jws', 'hash').limit(1)
        .then(function(res) {
          if (res.length && p.ipfs) {
            hash = res[0].hash;
            return pub.addMessageToIpfs(res[0]);
          }
          return [];
        })
        .then(function(res) {
          if (res.length) {
            return knex('Messages').where({ hash: hash }).update({ ipfs_hash: res[0].hash });
          }
        })
        .then(function(res) {
          if (parseInt(res)) {
            counter += 1;
            return getReindexQuery();
          }
        })
        .catch(function(e) { console.log('adding to ipfs failed:', e); });
      }

      return getReindexQuery().then(function() {
        return 'Reindexed ' + counter + ' messages';
      });
    },

    addIdentityIndexToIpfs: function() {
      // Stub
    },

    addMessageIndexToIpfs: function() {
      var limit = 100;
      var offset = 0;
      var distance = 0;
      var totalMsgs = 0;
      var maxMsgCount = 1000;
      var maxDepth = 10;
      var hashes = [];
      function iterate(limit, offset, distance) {
        return knex('Messages')
        .innerJoin('MessageAttributes as author', function() {
          this.on('author.message_hash', '=', 'Messages.hash');
          this.andOn('author.is_recipient', '=', knex.raw('?', false));
        })
        .innerJoin('TrustDistances as td', function() {
          this.on('td.start_attr_name', '=', knex.raw('?', myId[0]));
          this.on('td.start_attr_value', '=', knex.raw('?', myId[1]));
          this.on('td.end_attr_name', '=', 'author.name');
          this.on('td.end_attr_value', '=', 'author.value');
        })
        .where('td.distance', distance)
        .andWhere('public', true)
        .select('jws')
        .limit(limit)
        .offset(offset)
        .then(function(msgs) {
          if (msgs.length === 0 && offset === 0) {
            return;
          }
          if (msgs.length < limit) {
            distance += 1;
            offset = 0;
          } else {
            offset += limit;
          }
          var i;
          var q = new P(function(resolve) { resolve(msgs.length); });
          if (!p.ipfs) {
            return 'No IPFS connection';
          }
          function getSaveToIpfsFunction(message) {
            return function() {
              process.stdout.write(".");
              return pub.addMessageToIpfs(message)
              .then(function (res) {
                if (res.length && res[0].hash) {
                  hashes.push(res[0].hash);
                }
              })
              .catch(function(e) {
                console.log('adding message to ipfs failed:', e);
              });
            };
          }
          for (i = 0; i < msgs.length; i++) {
            totalMsgs += 1;
            if (totalMsgs >= maxMsgCount) { break; }
            q = q.then(getSaveToIpfsFunction(msgs[i]));
          }
          return q;
        })
        .then(function(res) {
          if (totalMsgs <= maxMsgCount) {
            return iterate(limit, offset, distance);
          }
          return totalMsgs;
        });
      }

      console.log('adding msgs to ipfs');
      return iterate(limit, offset, distance)
      .then(function(res) {
        // Add message index to IPFS
        console.log('adding', hashes.length, 'message hashes to ipfs');
        return p.ipfs.files.add(new Buffer(JSON.stringify(hashes), 'utf8'));
      })
      .then(function(res) {
        if (p.ipfs.name && res.length && res[0].hash) {
          console.log(res);
          console.log('publishing index', res[0].hash);
          return p.ipfs.name.publish(res[0].hash, {});
        } else {
          return res;
        }
      })
      .then(function(res) {
        console.log('published index', res);
        return res;
      })
      .catch(function(e) {
        console.log('error publishing index', e);
      });
    },

    saveMessageFromIpfs: function(path) {
      return p.ipfs.files.cat(path, { buffer: true })
      .then(function(buffer) {
        var msg = { jws: buffer.toString('utf8'), ipfs_hash: path };
        Message.verify(msg);
        console.log('saving msg from ipfs:', msg.hash);
        return pub.saveMessage(msg);
      })
      .catch(function(e) {
        console.log('Processing message', path, 'failed:', e);
      });
    },

    saveMessagesFromIpfsIndex: function(ipnsName) {
      console.log('Getting path for name', ipnsName);
      if (!(p.ipfs && p.ipfs.name)) {
        console.log('ipfs.name is not available');
        return;
      }
      return p.ipfs.name.resolve(ipnsName)
      .then(function(res) {
        var path = res['Path'].replace('/ipfs/', '');
        console.log('resolved name', path);
        return p.ipfs.files.cat(path, { buffer: true });
      })
      .then(function(buffer) {
        console.log(buffer.toString('utf8'));
        var msgs = JSON.parse(buffer.toString('utf8'));
        var i;
        var q = new P(function(resolve) { resolve(); });
        function getFn(path) {
          return function() {
            return pub.saveMessageFromIpfs(path);
          };
        }
        for (i = 0; i < msgs.length; i++) {
          q = q.then(getFn(msgs[i]));
        }
        return q;
      })
      .then(function() {
        console.log('Finished saving messages from index', ipnsName);
      })
      .catch(function(e) {
        console.log('Processing index', ipnsName, 'failed:', e);
      });
    },

    saveMessagesFromIpfsIndexes: function() {
      return knex('Messages')
      .innerJoin('MessageAttributes as author', function() {
        this.on('author.message_hash', '=', 'Messages.hash');
        this.andOn('author.is_recipient', '=', knex.raw('?', false));
      })
      .innerJoin('MessageAttributes as recipient', function() {
        this.on('recipient.message_hash', '=', 'Messages.hash');
        this.andOn('recipient.is_recipient', '=', knex.raw('?', true));
      })
      .innerJoin('TrustDistances as td', function() {
        this.on('td.start_attr_name', '=', knex.raw('?', myId[0]));
        this.on('td.start_attr_value', '=', knex.raw('?', myId[1]));
        this.on('td.end_attr_name', '=', 'author.name');
        this.on('td.end_attr_value', '=', 'author.value');
      })
      .where('td.distance', '<=', 2)
      .andWhere('recipient.name', 'nodeID')
      .select()
      .distinct('recipient.value')
      .then(function(res) {
        var i;
        var q = new P(function(resolve) { resolve(); });
        for (i = 0; i < res.length; i++) {
          q = q.then(pub.saveMessagesFromIpfsIndex(res[i].value));
        }
        return q;
      })
      .catch(function(e) {
        console.log('Saving messages from indexes failed:', e);
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

      var authorIdentityIdQuery = new P(function(resolve) { resolve([]); }),
        recipientIdentityIdQuery = new P(function(resolve) { resolve([]); });
      if (options.viewpoint) {
        if (options.author) {
          authorIdentityIdQuery = knex('IdentityAttributes')
            .where({
              name: options.author[0],
              value: options.author[1],
              viewpoint_name: options.viewpoint[0],
              viewpoint_value: options.viewpoint[1]
            })
            .select('identity_id');
        }

        if (options.recipient) {
          recipientIdentityIdQuery = knex('IdentityAttributes')
            .where({
              name: options.recipient[0],
              value: options.recipient[1],
              viewpoint_name: options.viewpoint[0],
              viewpoint_value: options.viewpoint[1]
            })
            .select('identity_id');
        }
      }

      return P.all([authorIdentityIdQuery, recipientIdentityIdQuery]).then(function(response) {
        var authorIdentityId = response[0].length > 0 && response[0][0].identity_id;
        var recipientIdentityId = response[1].length > 0 && response[1][0].identity_id;
        var select = ['Messages.*'];
        if (options.viewpoint) {
          select.push(knex.raw('MIN(td.distance) AS "distance"'));
          select.push(knex.raw('MAX(st.positive_score) AS author_pos'));
          select.push(knex.raw('MAX(st.negative_score) AS author_neg'));
        }
        var query = knex.select(select)
          .groupBy('Messages.hash')
          .from('Messages')
          .innerJoin('MessageAttributes as author', function() {
            this.on('Messages.hash', '=', 'author.message_hash');
            this.on('author.is_recipient', '=', knex.raw('?', false));
          })
          .innerJoin('MessageAttributes as recipient', function() {
            this.on('Messages.hash', '=', 'recipient.message_hash');
            this.andOn('recipient.is_recipient', '=', knex.raw('?', true));
          })
          .orderBy(options.orderBy, options.direction)
          .limit(options.limit)
          .offset(options.offset);

        if (options.distinctAuthor) {
          // group by author
        }

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

        if (options.where['hash']) {
          var hash = options.where['hash'];
          query.where(function() {
            this.where('Messages.hash', hash);
            this.orWhere('Messages.ipfs_hash', hash);
          });
          delete options.where['hash'];
        }

        if (options.viewpoint) {
          // Replace left joins with subquery for performance?
          query.leftJoin('IdentityAttributes as author_attribute', function() {
            this.on('author_attribute.name', '=', 'author.name');
            this.on('author_attribute.value', '=', 'author.value');
            this.on('author_attribute.viewpoint_name', '=', knex.raw('?', options.viewpoint[0]));
            this.on('author_attribute.viewpoint_value', '=', knex.raw('?', options.viewpoint[1]));
          });
          query.leftJoin('IdentityStats as st', 'st.identity_id', 'author_attribute.identity_id');
          query.leftJoin('IdentityAttributes as recipient_attribute', function() {
            this.on('recipient_attribute.name', '=', 'recipient.name');
            this.on('recipient_attribute.value', '=', 'recipient.value');
            this.on('recipient_attribute.viewpoint_name', '=', knex.raw('?', options.viewpoint[0]));
            this.on('recipient_attribute.viewpoint_value', '=', knex.raw('?', options.viewpoint[1]));
          });
          query.innerJoin('TrustDistances as td', function() {
            this.on('author_attribute.name', '=', 'td.end_attr_name')
              .andOn('author_attribute.value', '=', 'td.end_attr_value')
              .andOn('td.start_attr_name', '=', knex.raw('?', options.viewpoint[0]))
              .andOn('td.start_attr_value', '=', knex.raw('?', options.viewpoint[1]));
            if (options.maxDistance > 0) {
              this.andOn('td.distance', '<=', knex.raw('?', options.maxDistance));
            }
          });
          if (options.maxDistance > 0) {
            query.where('Messages.priority', '>', 0);
          }
          if (options.author) {
            // Extend message search to other attributes connected to the author
            if (authorIdentityId) {
              query.where('author_attribute.identity_id', authorIdentityId);
            } else {
              query.where('author.name', options.author[0]);
              query.where('author.value', options.author[1]);
            }
          }
          if (options.recipient) {
            if (recipientIdentityId) {
              query.where('recipient_attribute.identity_id', recipientIdentityId);
            } else {
              query.where('recipient.name', options.recipient[0]);
              query.where('recipient.value', options.recipient[1]);
            }
          }
        } else {
          if (options.author) {
            options.where['author.name'] = options.author[0];
            options.where['author.value'] = options.author[1];
          }
          if (options.recipient) {
            options.where['recipient.name'] = options.recipient[0];
            options.where['recipient.value'] = options.recipient[1];
          }
        }
        query.where(options.where);

        return query;
      });
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
        subquery.where(knex.raw('lower("attr"."value")'), 'LIKE', '%' + options.searchValue.toLowerCase() + '%');
      }

      // subquery.then(function(res) { console.log(JSON.stringify(res, null, 2)); });
      var q = knex.from('IdentityAttributes AS attr')
        .select([
          'attr.identity_id',
          'attr.name',
          'attr.value',
          'attr.confirmations',
          'td.distance',
          'st.positive_score',
          'st.negative_score'
        ])
        .leftJoin('TrustDistances as td', function() {
          this.on('td.start_attr_name', '=', 'attr.viewpoint_name');
          this.andOn('td.start_attr_value', '=', 'attr.viewpoint_value');
          this.andOn('td.end_attr_name', '=', 'attr.name');
          this.andOn('td.end_attr_value', '=', 'attr.value');
        })
        .leftJoin('IdentityStats as st', function() {
          this.on('attr.identity_id', '=', 'st.identity_id');
        })
        .where('attr.identity_id', 'in', subquery);

      // sql += "ORDER BY iid >= 0 DESC, IFNULL(tp.Distance,1000) ASC, CASE WHEN attrvalue LIKE :query || '%' THEN 0 ELSE 1 END, UID.name IS NOT NULL DESC, attrvalue ASC ";

      return q.then(function(res) {
        // console.log(JSON.stringify(res, null, 2));
        var identities = {};
        var i;
        for (i = 0; i < res.length; i++) {
          var attr = res[i];
          identities[attr.identity_id] = identities[attr.identity_id] || [];
          identities[attr.identity_id].push({
            attr: attr.name,
            val: attr.value,
            dist: attr.distance,
            pos: attr.positive_score,
            neg: attr.negative_score
          });
        }

        var arr = [];
        for (var key in identities) {
          arr.push(identities[key]);
        }

        // Sort by distance
        arr = arr.sort(function(identity1, identity2) {
          var smallestDistance1 = 1000, smallestDistance2 = 1000;
          for (i = 0; i < identity1.length; i++) {
            if (!isNaN(parseInt(identity1[i].dist)) && identity1[i].dist < smallestDistance1) {
              smallestDistance1 = identity1[i].dist;
            }
          }
          for (i = 0; i < identity2.length; i++) {
            if (!isNaN(parseInt(identity2[i].dist)) && identity2[i].dist < smallestDistance2) {
              smallestDistance2 = identity2[i].dist;
            }
          }
          return smallestDistance1 - smallestDistance2;
        });

        return new P(function(resolve) {
          resolve(arr);
        });
      });
    },

    mapIdentityAttributes: function(options) {
      var countBefore, identityId, sql;
      options.viewpoint = options.viewpoint || myId;
      // Find out existing identity_id for the identifier
      var getExistingId = knex.from('IdentityAttributes as ia')
        .select('identity_id')
        .where({
          'ia.name': options.id[0],
          'ia.value': options.id[1],
          viewpoint_name: options.viewpoint[0],
          viewpoint_value: options.viewpoint[1]
        })
        .innerJoin('UniqueIdentifierTypes as uidt', 'uidt.name', 'ia.name');
      var existingId;

      return getExistingId.then(function(res) {
        existingId = res;
        return knex('IdentityAttributes')
          // Delete previously saved attributes of the identity_id
          .where('identity_id', 'in', getExistingId).del()
          .then(function(res) {
            if (existingId.length) {
              // Pass on the existing identity_id
              return new P(function(resolve) { resolve(existingId); });
            } else {
              return knex('IdentityAttributes')
              // No existing identity_id - return a new one
                .select(knex.raw(SQL_IFNULL + "(MAX(identity_id), 0) + 1 AS identity_id"));
            }
          })
          .then(function(res) {
            identityId = parseInt(res[0].identity_id);
            // First insert the queried identifier with the identity_id
            return knex('IdentityAttributes').insert({
              identity_id: identityId,
              name: options.id[0],
              value: options.id[1],
              viewpoint_name: options.viewpoint[0],
              viewpoint_value: options.viewpoint[1],
              confirmations: 1,
              refutations: 0
            });
          })
          .then(function() {
            var p, last;
            function generateSubQuery() {
              return knex
                .from('Messages as m')
                .innerJoin('MessageAttributes as attr1', function() {
                  this.on('m.hash', '=', 'attr1.message_hash');
                })
                .innerJoin('IdentityAttributes as ia', function() {
                  this.on('ia.name', '=', 'attr1.name');
                  this.on('ia.value', '=', 'attr1.value');
                  this.on('ia.identity_id', '=', identityId);
                })
                .innerJoin('MessageAttributes as attr2', function() {
                  this.on('m.hash', '=', 'attr2.message_hash');
                  this.on('attr2.is_recipient', '=', 'attr1.is_recipient');
                })
                .innerJoin('UniqueIdentifierTypes as uidt', 'uidt.name', 'attr1.name')
                .innerJoin('TrustDistances as td_signer', function() {
                  this.on('td_signer.start_attr_name', '=', knex.raw('?', options.viewpoint[0]));
                  this.on('td_signer.start_attr_value', '=', knex.raw('?', options.viewpoint[1]));
                  this.on('td_signer.end_attr_name', '=', knex.raw('?', 'keyID'));
                  this.on('td_signer.end_attr_value', '=', 'm.signer_keyid');
                });
            }

            function generateDeleteSubQuery() {
              return generateSubQuery()
                // Select for deletion the related identity attributes that were previously inserted
                // with a different identity_id
                .innerJoin('IdentityAttributes as existing', function() {
                  this.on('existing.identity_id', '!=', identityId);
                  this.on('existing.name', '=', 'attr2.name');
                  this.on('existing.value', '=', 'attr2.value');
                  this.on('existing.viewpoint_name', '=', knex.raw('?', options.viewpoint[0]));
                  this.on('existing.viewpoint_value', '=', knex.raw('?', options.viewpoint[1]));
                })
                .innerJoin('UniqueIdentifierTypes as uidt2', 'uidt2.name', 'existing.name')
                .select('existing.identity_id');
            }

            function generateInsertSubQuery() {
              return generateSubQuery()
                // Select for insertion the related identity attributes that do not already exist
                // on the identity_id
                .leftJoin('IdentityAttributes as existing', function() {
                  this.on('existing.identity_id', '=', identityId);
                  this.on('existing.name', '=', 'attr2.name');
                  this.on('existing.value', '=', 'attr2.value');
                  this.on('existing.viewpoint_name', '=', knex.raw('?', options.viewpoint[0]));
                  this.on('existing.viewpoint_value', '=', knex.raw('?', options.viewpoint[1]));
                })
                .whereNull('existing.identity_id')
                .select(
                  identityId,
                  'attr2.name',
                  'attr2.value',
                  knex.raw('?', options.viewpoint[0]),
                  knex.raw('?', options.viewpoint[1]),
                  knex.raw('SUM(CASE WHEN m.type = \'verify_identity\' THEN 1 ELSE 0 END)'),
                  knex.raw('SUM(CASE WHEN m.type = \'unverify_identity\' THEN 1 ELSE 0 END)')
                ).groupBy('attr2.name', 'attr2.value');
            }

            function iterateSearch() {
              return knex('IdentityAttributes').whereIn('identity_id', generateDeleteSubQuery()).del()
              .then(function(res) {
                return knex('IdentityAttributes').insert(generateInsertSubQuery());
              })
              .then(function(res) {
                if (JSON.stringify(last) !== JSON.stringify(res)) {
                  last = res;
                  return iterateSearch();
                }
              });
            }

            return iterateSearch();
          })
          .then(function(res) {
            var hasSearchedAttributes = options.searchedAttributes && options.searchedAttributes.length > 0;

            if (hasSearchedAttributes) {
              return knex('IdentityAttributes')
                .select('name', 'value', 'confirmations', 'refutations')
                .where(knex.raw('NOT (Name = ? AND value = ?) AND identity_id = ?', [options.id[0], options.id[1], identityId]))
                .whereIn('name', options.searchedAttributes)
                .orderByRaw('confirmations - refutations DESC');
            }

            return knex('IdentityAttributes')
              .select('name', 'value', 'confirmations', 'refutations')
              .where(knex.raw('NOT (Name = ? AND value = ?) AND identity_id = ?', [options.id[0], options.id[1], identityId]))
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
      var trustedKey = trustedKeyID ? ['keyID', trustedKeyID] : id;

      /*
        Can create TrustDistances based on messages authored by startId, or transitively, from messages
        authored by identities that have a TrustDistance from startId (when depth > 1)
      */
      function buildQuery(betweenKeyIDsOnly, trx, depth) {
        var startId = betweenKeyIDsOnly ? trustedKey : id;
        var subQuery = trx.distinct(
          trx.raw('?', startId[0]),
          trx.raw('?', startId[1]),
          'attr2.name',
          'attr2.value',
          depth
        )
          .select()
          .from('Messages as m')
          .innerJoin('MessageAttributes as attr1', function() {
            this.on('m.hash', '=', 'attr1.message_hash');
            if (depth === 1) {
              this.on('attr1.name', '=', trx.raw('?', startId[0]));
              this.on('attr1.value', '=', trx.raw('?', startId[1]));
            }
            this.on('attr1.is_recipient', '=', trx.raw('?', false));
          })
          .innerJoin('MessageAttributes as attr2', function() {
            this.on('m.hash', '=', 'attr2.message_hash');
            if (betweenKeyIDsOnly) {
              this.on('attr2.name', '=', trx.raw('?', 'keyID'));
            }
            this.on('attr2.is_recipient', '=', trx.raw('?', true));
          });

        if (depth > 1) {
          subQuery.innerJoin('TrustDistances as td_author', function() {
            this.on('td_author.start_attr_name', '=', trx.raw('?', startId[0]));
            this.on('td_author.start_attr_value', '=', trx.raw('?', startId[1]));
            this.on('td_author.end_attr_name', '=', 'attr1.name');
            this.on('td_author.end_attr_value', '=', 'attr1.value');
            this.on('td_author.distance', '=', trx.raw('?', depth - 1));
          });
        }

        // Where not exists
        subQuery.leftJoin('TrustDistances as td_recipient', function() {
          this.on('td_recipient.start_attr_name', '=', trx.raw('?', startId[0]));
          this.on('td_recipient.start_attr_value', '=', trx.raw('?', startId[1]));
          this.on('td_recipient.end_attr_name', '=', 'attr2.name');
          this.on('td_recipient.end_attr_value', '=', 'attr2.value');
        });
        subQuery.whereNull('td_recipient.distance');

        if (!betweenKeyIDsOnly) {
          subQuery.innerJoin('UniqueIdentifierTypes as uidt1', 'uidt1.name', 'attr1.name');
          subQuery.innerJoin('UniqueIdentifierTypes as uidt2', 'uidt2.name', 'attr2.name');
          subQuery.leftJoin('TrustDistances as td_signer', function() {
            this.on('td_signer.start_attr_name', '=', trx.raw('?', trustedKey[0]));
            this.on('td_signer.start_attr_value', '=', trx.raw('?', trustedKey[1]));
            this.on('td_signer.end_attr_name', '=', trx.raw('?', 'keyID'));
            this.on('td_signer.end_attr_value', '=', 'm.signer_keyid');
          });
          subQuery.where(function() {
            this.whereNotNull('td_signer.distance').orWhere('m.signer_keyid', trustedKey[1]);
          });
        }
        subQuery.where('m.is_latest', true)
          .andWhere('m.rating', '>', trx.raw('(m.min_rating + m.max_rating) / 2'));

        return trx('TrustDistances').insert(subQuery).return();
      }

      var q, q2;
      if (maintain) {
        q = this.addTrustIndexedAttribute(id, maxDepth);
      } else {
        q = new P(function(resolve) { resolve(); });
      }
      q = q.then(function() {
        return pub.mapIdentityAttributes({ id: id, viewpoint: id });
      });
      var i;
      return q.then(function() {
        return knex.transaction(function(trx) {
          return trx('TrustDistances')
            .where({ start_attr_name: id[0], start_attr_value: id[1] }).del()
            .then(function() {
              return trx('TrustDistances').where({ start_attr_name: trustedKey[0], start_attr_value: trustedKey[1] }).del();
            })
            .then(function(res) {
              // Add trust distance to self = 0
              return trx('TrustDistances')
                .insert({ start_attr_name: id[0], start_attr_value: id[1], end_attr_name: id[0], end_attr_value: id[1], distance: 0 })
                .then(function() {
                  if (trustedKey[0] !== id[0] && trustedKey[1] !== id[1]) {
                    return trx('TrustDistances')
                      .insert({ start_attr_name: trustedKey[0], start_attr_value: trustedKey[1], end_attr_name: trustedKey[0], end_attr_value: trustedKey[1], distance: 0 })
                      .return();
                  }
                });
            })
            .then(function() {
              q2 = new P(function(resolve) { resolve(); });
              for (i = 1; i <= maxDepth; i++) {
                q2.then(buildQuery(true, trx, i));
              }
              return q2;
            })
            .then(function() {
              q2 = new P(function(resolve) { resolve(); });
              for (i = 1; i <= maxDepth; i++) {
                q2.then(buildQuery(false, trx, i));
              }
              return q2;
            })
            .then(function() {
              return trx('TrustDistances')
                .where({ start_attr_name: id[0], start_attr_value: id[1] })
                .count('* as wot_size');
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

    getTrustPaths: function(start, end, maxLength, shortestOnly, viewpoint, limit) {
      return new P(function(resolve) { resolve([]); }); // Disabled until the performance is improved

    },

    getMessageCount: function() {
      return knex('Messages').count('* as val')
      .then(function(res) {
        return parseInt(res[0].val);
      });
    },

    getStats: function(id, options) {
      var sentSql = "";
      sentSql += "SUM(CASE WHEN m.rating > (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS sent_positive, ";
      sentSql += "SUM(CASE WHEN m.rating = (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS sent_neutral, ";
      sentSql += "SUM(CASE WHEN m.rating < (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS sent_negative ";

      var dbTrue = true, dbFalse = false;
      if (config.db.dialect === 'sqlite3') { // sqlite in test env fails otherwise, for some reason
        dbTrue = 1;
        dbFalse = 0;
      }

      var sent = knex('Messages as m')
        .innerJoin('MessageAttributes as author', function() {
          this.on('author.message_hash', '=', 'm.hash');
          this.on('author.is_recipient', '=', knex.raw('?', dbFalse));
        })
        .where('m.type', 'rating')
        .where('m.public', dbTrue);

      var receivedSql = '';
      receivedSql += "SUM(CASE WHEN m.rating > (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS received_positive, ";
      receivedSql += "SUM(CASE WHEN m.rating = (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS received_neutral, ";
      receivedSql += "SUM(CASE WHEN m.rating < (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS received_negative, ";
      receivedSql += "MIN(m.timestamp) AS first_seen ";

      var received = knex('Messages as m')
        .innerJoin('MessageAttributes as recipient', function() {
          this.on('recipient.message_hash', '=', 'm.hash');
          this.on('recipient.is_recipient', '=', knex.raw('?', dbTrue));
        })
        .where('m.type', 'rating')
        .where('m.public', dbTrue);

      var identityId, identityIdQuery = new P(function(resolve) { resolve(); });
      if (options.viewpoint && options.maxDistance > -1) {
        identityIdQuery = knex('IdentityAttributes')
          .where({
            name: id[0],
            value: id[1],
            viewpoint_name: options.viewpoint[0],
            viewpoint_value: options.viewpoint[1]
          })
          .select('identity_id');

        identityIdQuery.then(function(res) {
          if (res.length) {
            identityId = res[0].identity_id;

            sent.select('ia.identity_id as identity_id', 'm.hash');
            sent.innerJoin('IdentityAttributes as ia', function() {
              this.on('author.name', '=', 'ia.name');
              this.on('author.value', '=', 'ia.value');
            });
            sent.where('ia.identity_id', identityId);
            sent.where('m.priority', '>', 0);
            sent.groupBy('m.hash', 'ia.identity_id');

            var sentSubquery = knex.raw(sent).wrap('(', ') s');
            sent = knex('Messages as m')
              .select(knex.raw(sentSql))
              .innerJoin(sentSubquery, 'm.hash', 's.hash')
              .groupBy('s.identity_id');

            received.select('ia.identity_id as identity_id', 'm.hash as hash');
            received.innerJoin('IdentityAttributes as ia', function() {
              this.on('recipient.name', '=', 'ia.name');
              this.on('recipient.value', '=', 'ia.value');
            });
            received.where('ia.identity_id', identityId);
            received.groupBy('m.hash', 'ia.identity_id');

            received.innerJoin('TrustDistances as td', function() {
              this.on('td.start_attr_name', '=', knex.raw('?', options.viewpoint[0]));
              this.andOn('td.start_attr_value', '=', knex.raw('?', options.viewpoint[1]));
              this.andOn('td.end_attr_name', '=', 'ia.name');
              this.andOn('td.end_attr_value', '=', 'ia.value');
              if (options.maxDistance > 0) {
                this.andOn('td.distance', '<=', options.maxDistance);
              }
            });

            var receivedSubquery = knex.raw(received).wrap('(', ') s');
            received = knex('Messages as m')
              .select(knex.raw(receivedSql))
              .innerJoin(receivedSubquery, 'm.hash', 's.hash')
              .groupBy('s.identity_id');
          }
        });
      }
      return identityIdQuery.then(function() {
        if (!identityId) {
          sent.where({ 'author.name': id[0], 'author.value': id[1] });
          sent.groupBy('author.name', 'author.value');
          sent.select(knex.raw(sentSql));
          received.where({ 'recipient.name': id[0], 'recipient.value': id[1] });
          received.groupBy('recipient.name', 'recipient.value');
          received.select(knex.raw(receivedSql));
        }

        return new P.all([sent, received]).then(function(response) {
          var res = Object.assign({}, response[0][0], response[1][0]);
          for (var key in res) {
            if (key.indexOf('sent_') === 0 || key.indexOf('received_') === 0) {
              res[key] = parseInt(res[key]);
            }
          }

          if (options.viewpoint && !options.maxDistance) {
            var identityIds = [];
            if (identityId) {
              identityIds.push(identityId);
            }
            knex('IdentityStats')
            .where('identity_id', 'in', identityIds)
            .delete()
            .then(function() {
              if (identityId) {
                knex('IdentityStats')
                  .insert({
                    identity_id: identityId,
                    viewpoint_name: options.viewpoint[0],
                    viewpoint_value: options.viewpoint[1],
                    positive_score: res.received_positive || 0,
                    negative_score: res.received_negative || 0
                  }).return();
              }
            });
          }

          return new P(function(resolve) {
            return resolve(res);
          });
        });
      });
    },

    checkDefaultTrustList: function(db) {
      var _ = this;
      return knex('Messages').count('* as count')
      .then(function(res) {
        if (parseInt(res[0].count) === 0) {
          var queries = [];
          var message = Message.createRating({
            author: [myId],
            recipient: [['keyID', '/pbxjXjwEsojbSfdM3wGWfE24F4fX3GasmoHXY3yYPM=']],
            comment: 'Identifi seed node, trusted by default',
            rating: 10,
            context: 'identifi_network',
            public: false
          });
          var message2 = Message.createRating({
            author: [myId],
            recipient: [['nodeID', 'Qmbb1DRwd75rZk5TotTXJYzDSJL6BaNT1DAQ6VbKcKLhbs']],
            comment: 'Identifi IPFS seed node, trusted by default',
            rating: 10,
            context: 'identifi_network',
            public: false
          });
          Message.sign(message, myKey.private.pem, myKey.public.hex);
          Message.sign(message2, myKey.private.pem, myKey.public.hex);
          queries.push(_.saveMessage(message));
          queries.push(_.saveMessage(message2));
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
        .innerJoin('UniqueIdentifierTypes', 'IdentityAttributes.name', 'UniqueIdentifierTypes.name')
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
      // Find existing or new identity_id for message recipient UniqueIdentifierTypes
      // If the attribute exists on the identity_id, increase confirmations or refutations
      // If the attribute doesn't exist, add it with 1 confirmation or refutation
    },

    deletePreviousMessage: function(message) {
      var i, j;
      var verifyTypes = ['verify_identity', 'unverify_identity'], t = message.signedData.type;
      var isVerifyMsg = verifyTypes.indexOf(t) > -1;

      function getHashesQuery(author, recipient) {
        var q = knex('Messages as m')
          .distinct('m.hash as hash', 'm.timestamp')
          .innerJoin('MessageAttributes as author', function() {
            this.on('author.message_hash', '=', 'm.hash');
            this.andOn('author.is_recipient', '=', knex.raw('?', false));
          })
          .innerJoin('MessageAttributes as recipient', function() {
            this.on('recipient.message_hash', '=', 'm.hash');
            this.andOn('recipient.is_recipient', '=', knex.raw('?', true));
          })
          .innerJoin('UniqueIdentifierTypes as ia1', 'ia1.name', 'author.name')
          .where({
            'm.signer_keyid': message.signerKeyHash,
            'author.name': author[0],
            'author.value': author[1]
          })
          .orderBy('m.timestamp', 'DESC');

        if (recipient) {
          q.where({
            'recipient.name': recipient[0],
            'recipient.value': recipient[1]
          });
          q.innerJoin('UniqueIdentifierTypes as ia2', 'ia2.name', 'recipient.name');
        }

        if (isVerifyMsg) {
          q.whereIn('m.type', verifyTypes);
          if (recipient) {
            q.offset(10); // Accept up to 10 verification msgs from A -> B
          }
        } else {
          q.where('m.type', t);
        }

        return q;
      }

      var hashes = [];

      function addHashes(res) {
        for (var i = 0; i < res.length; i++) {
          hashes.push(res[i].hash);
        }
      }

      function getAndAddHashes(author, recipient) {
        return getHashesQuery(author, recipient)
          .then(addHashes);
      }

      function addInnerJoinMessageRecipient(query, recipient, n) {
        var as = 'recipient' + n;
        query.innerJoin('MessageAttributes as ' + as, function() {
          this.on(as + '.message_hash', '=', 'm.hash');
          this.on(as + '.is_recipient', '=', knex.raw('?', true));
          this.on(as + '.name', '=', knex.raw('?', recipient[0]));
          this.on(as + '.value', '=', knex.raw('?', recipient[1]));
        });
      }

      var queries = [];

      // Delete previous verify or unverify with the exact same recipient attributes
      if (isVerifyMsg) {
        for (i = 0; i < message.signedData.author.length; i++) {
          var q = getHashesQuery(message.signedData.author[i]);
          for (j = 0; j < message.signedData.recipient.length; j++) {
            addInnerJoinMessageRecipient(q, message.signedData.recipient[j], j);
          }
          queries.push(q.then(addHashes));
        }
      }

      // Delete possible previous msg from A->B (created less than minMessageInterval ago?)
      for (i = 0; i < message.signedData.author.length; i++) {
        for (j = 0; j < message.signedData.recipient.length; j++) {
          queries.push(getAndAddHashes(message.signedData.author[i],  message.signedData.recipient[j]));
        }
      }

      return P.all(queries).then(function() {
        queries = [];
        hashes = util.removeDuplicates(hashes);
        for (i = 0; i < hashes.length; i++) {
          queries.push(pub.dropMessage(hashes[i]));
        }
        return P.all(queries);
      });
    },

    updateWotIndexesByMessage: function(message) {
      var queries = [];

      // TODO: remove trust distance if a previous positive rating is replaced

      function makeSubquery(author, recipient) {
        return knex
        .from('TrustIndexedAttributes AS viewpoint')
        .innerJoin('UniqueIdentifierTypes as ia', 'ia.name', knex.raw('?', recipient[0]))
        .innerJoin('TrustDistances AS td', function() {
          this.on('td.start_attr_name', '=', 'viewpoint.name')
          .andOn('td.start_attr_value', '=', 'viewpoint.value')
          .andOn('td.end_attr_name', '=', knex.raw('?', author[0]))
          .andOn('td.end_attr_value', '=', knex.raw('?', author[1]));
        })
        .leftJoin('TrustDistances AS existing', function() { // TODO: update existing if new distance is shorter
          this.on('existing.start_attr_name', '=', 'viewpoint.name')
          .andOn('existing.start_attr_value', '=', 'viewpoint.value')
          .andOn('existing.end_attr_name', '=', knex.raw('?', recipient[0]))
          .andOn('existing.end_attr_value', '=', knex.raw('?', recipient[1]));
        })
        .whereNull('existing.distance')
        .select(
          'viewpoint.name as start_attr_name',
          'viewpoint.value as start_attr_value',
          knex.raw('? as end_attr_name', recipient[0]),
          knex.raw('? as end_attr_value', recipient[1]),
          knex.raw(SQL_IFNULL+'(td.distance, 0) + 1 as distance')
        );
      }

      function getSaveFunction(author, recipient) {
        return function(distance) {
          if (distance > -1) {
            return knex('TrustDistances').insert(makeSubquery(author, recipient));
          }
        };
      }

      if (Message.isPositive(message)) {
        var i, j;
        for (i = 0; i < message.signedData.author.length; i++) {
          var author = message.signedData.author[i];
          var t = author[0] === 'keyID' ? author[1] : myKey.hash; // trusted key
          for (j = 0; j < message.signedData.recipient.length; j++) {
            var recipient = message.signedData.recipient[j];
            var q = pub.getTrustDistance(['keyID', t], ['keyID', message.signerKeyHash])
            .then(getSaveFunction(author, recipient));
            queries.push(q);
          }
        }
      }

      return P.all(queries);
    }
  };

  pub.init = function(conf, ipfs) {
    p.ipfs = ipfs;
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
        return pub.mapIdentityAttributes({ id: myId });
      })
      .then(function() {
        return pub.checkDefaultTrustList();
      })
      .then(function() {
        pub.saveMessagesFromIpfsIndexes(); // non-blocking
      });
  };

  return pub;
};
