/*jshint unused: false */
'use strict';
var Promise = require("bluebird");
var util = require('./util.js');
var schema = require('./schema.js');

var crypto = require('crypto');
var moment = require('moment');
var btree = require('merkle-btree');

var Message = require('identifi-lib/message');
var keyutil = require('identifi-lib/keyutil');
var myKey = keyutil.getDefault();
var myId = ['keyID', myKey.hash];
var myTrustIndexDepth = 4;
var config;
var ipfsIndexWidth = 200;

var dagPB = require('ipld-dag-pb');

var SQL_IFNULL = 'IFNULL';
var SQL_INSERT_OR_REPLACE = 'INSERT OR REPLACE';
var SQL_ON_CONFLICT = '';
var SQL_PRINTF = 'PRINTF';

function sortByKey(a, b) {
  if (a.key < b.key) {
    return -1;
  }
  if (a.key > b.key) {
    return 1;
  }
  return 0;
}

function timeoutPromise(promise, timeout) {
  return Promise.race([
    promise,
    new Promise(function(resolve) {
      setTimeout(function() {
        //console.log('promise timed out');
        resolve();
      }, timeout);
    })
  ]);
}

module.exports = function(knex) {
  var p; // Private methods

  var lastIpfsIndexedMessageSavedAt = new Date().toISOString();

  var pub = {
    trustIndexedAttributes: null,
    saveMessage: function(message, updateTrustIndexes, addToIpfs) {
      if (typeof updateTrustIndexes === 'undefined') { updateTrustIndexes = true; }
      if (typeof addToIpfs === 'undefined') { addToIpfs = true; }
      if (typeof message.signerKeyHash === 'undefined') {
        message.signerKeyHash = Message.getSignerKeyHash(message);
      }
      var queries = [];

      var q = Promise.resolve();
      // Unobtrusively store msg to ipfs
      addToIpfs = (p.ipfs && !message.ipfs_hash) && addToIpfs;
      if (addToIpfs) {
        var identityIndexEntriesToAdd;
        q = pub.addMessageToIpfs(message)
        .then(function(res) {
          message.ipfs_hash = res[0].hash;
        });
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
            return Promise.resolve(false);
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
                saved_at:       new Date().toISOString(),
              })
              .then(function() {
                var i, queries = [];
                for (i = 0; i < message.signedData.author.length; i++) {
                  queries.push(trx('MessageAttributes').insert({
                    message_hash: message.hash,
                    name: message.signedData.author[i][0],
                    value: message.signedData.author[i][1],
                    is_recipient: false
                  } ));
                }
                for (i = 0; i < message.signedData.recipient.length; i++) {
                  queries.push(trx('MessageAttributes').insert({
                    message_hash: message.hash,
                    name: message.signedData.recipient[i][0],
                    value: message.signedData.recipient[i][1],
                    is_recipient: true
                  }));
                }
                return Promise.all(queries);
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
        }
      })
      .then(function() {
        return message;
      });

      return this.ensureFreeSpace().then(function() {
        return q;
      });
    },

    keepAddingNewMessagesToIpfsIndex: function() {
      return pub.addNewMessagesToIpfsIndex()
      .then(function() {
        return new Promise(function(resolve) {
          setTimeout(function() {
            resolve(pub.keepAddingNewMessagesToIpfsIndex());
          }, 10000);
        });
      });
    },

    addIndexesToIpfs: function() {
      return pub.addMessageIndexToIpfs()
      .then(function() {
        return pub.addIdentityIndexToIpfs();
      });
    },

    addNewMessagesToIpfsIndex: function() {
      return pub.getMessages({
        limit: 10000,
        orderBy: 'timestamp',
        direction: 'desc',
        viewpoint: myId,
        savedAtGt: lastIpfsIndexedMessageSavedAt
      })
      .then(function(messages) {
        if (messages.length) {
          console.log('', messages.length, 'new messages to index');
        }
        // rebuilding the indexes is more efficient than inserting large number of entries individually
        if (messages.length < 10) {
          var q = Promise.resolve();
          messages.forEach(function(message) {
            message = Message.decode(message);
            var d = new Date(message.saved_at).toISOString();
            if (d > lastIpfsIndexedMessageSavedAt) {
              lastIpfsIndexedMessageSavedAt = d;
            }
            q = q.then(function() {
              return pub.addMessageToIpfsIndex(message);
            });
          });
          return timeoutPromise(q.return(messages.length), 100000)
            .then(function(res) {
              if (typeof res === undefined) { return pub.addIndexesToIpfs(); }
              else { return res; }
            });
        } else {
          return pub.addIndexesToIpfs();
        }
      })
      .then(function(messagesAdded) {
        if (messagesAdded) {
          return pub.addIndexRootToIpfs();
        }
      })
      .catch(function(e) { console.log('adding new messages to ipfs failed:', e); });
    },

    addMessageToIpfsIndex: function(message) {
      var identityIndexEntriesToAdd = [], msgIndexKey = pub.getMsgIndexKey(message); // TODO: should have distance
      return p.ipfsMessagesByDistance.put(msgIndexKey, message)
      .then(function(res) {
        return p.ipfsMessagesByTimestamp.put(msgIndexKey.substr(msgIndexKey.indexOf(':') + 1), message);
      })
      .then(function() {
        return pub.getIdentityAttributes({ limit: 10, id: message.signedData.author[0] }); // TODO: make sure this is unique type
      })
      .then(function(authorAttrs) {
        var attrs = authorAttrs.length ? authorAttrs[0] : [];
        return pub.addIdentityToIpfsIndex(attrs);
      })
      .then(function() {
        return pub.getIdentityAttributes({ limit: 10, id: message.signedData.recipient[0] }); // TODO: make sure this is unique type - and get distance
      })
      .then(function(recipientAttrs) {
        var attrs = recipientAttrs.length ? recipientAttrs[0] : [];
        var shortestDistance = 99;
        attrs.forEach(function(attr) {
          if (typeof attr.dist === 'number' && attr.dist < shortestDistance) {
            shortestDistance = attr.dist;
          }
        });
        if (shortestDistance < 99) {
          message.distance = shortestDistance;
        }
        return pub.addIdentityToIpfsIndex(attrs);
      })
      .then(function() {
        var q = Promise.resolve();
        identityIndexEntriesToAdd.forEach(function(entry, i) {
          // console.log('start', i);
          q = q.then(function() {
            return p.ipfsIdentitiesByDistance.put(entry.key, entry.value);
          })
          .then(function() {
            return p.ipfsIdentitiesBySearchKey.put(entry.key.substr(entry.key.indexOf(':') + 1), entry.value);
          }).then(function() {
            // console.log('done', i)
          });
        });
        return q; // TODO: it aint returning
      })
      .catch(function(e) { console.log('adding to ipfs failed:', e); });
    },

    addMessageToIpfs: function(message) {
      return p.ipfs.files.add(new Buffer(message.jws, 'utf8'));
    },

    addDbMessagesToIpfs: function() {
      var counter = 0;
      var hash;

      function getReindexQuery() {
        var msgs = {};
        return knex('Messages').whereNull('ipfs_hash').select('jws', 'hash').limit(100)
        .then(function(res) {
          if (res.length && p.ipfs) {
            res.forEach(function(msg) {
              msgs[msg.hash] = new Buffer(msg.jws, 'utf8');
            });
            return p.ipfs.files.add(Object.values(msgs));
          }
          return [];
        })
        .then(function(res) {
          console.log('added', res.length, 'msgs to ipfs');
          var queries = [];
          Object.keys(msgs).forEach(function(hash, index) {
            queries.push(
              knex('Messages').where({ hash: hash }).update({ ipfs_hash: res[index].hash }).return()
            );
          });
          return Promise.all(queries);
        })
        .then(function(res) {
          console.log('updated', res.length, 'db entries');
          if (res.length) {
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

    addIndexRootToIpfs: function() {
      // saves indexes as an IPFS directory
      // TODO: if info already exists, don't rewrite
      var indexRoot;
      return pub.getIdentityAttributes({ id: myId })
      .then(function(attrs) {
        if (attrs.length) {
          attrs = attrs[0];
        } else {
          attrs = [];
        }
        return p.ipfs.files.add([
          { path: 'info', content: new Buffer(JSON.stringify({ keyID: myId[1], attrs: attrs })) },
          { path: 'messages_by_distance', content: new Buffer(p.ipfsMessagesByDistance.rootNode.serialize()) },
          { path: 'messages_by_timestamp', content: new Buffer(p.ipfsMessagesByTimestamp.rootNode.serialize()) },
          { path: 'identities_by_searchkey', content: new Buffer(p.ipfsIdentitiesBySearchKey.rootNode.serialize()) },
          { path: 'identities_by_distance', content: new Buffer(p.ipfsIdentitiesByDistance.rootNode.serialize()) }
        ]);
      })
      .then(function(res) {
        var links = [];
        for (var i = 0; i < res.length; i++) {
          links.push({ Name: res[i].path, Hash: res[i].hash, Size: res[i].size });
        }
        return new Promise(function(resolve, reject) {
          dagPB.DAGNode.create(new Buffer('\u0008\u0001'), links, function(err, dag) {
            if (err) {
              reject(err);
            }
            resolve(p.ipfs.object.put(dag));
          });
        });
      })
      .then(function(res) {
        if (res._json.multihash) {
          indexRoot = res._json.multihash;
          return knex('TrustIndexedAttributes')
            .where({ name: myId[0], value: myId[1] })
            .update('ipfs_index_root', res._json.multihash)
            .return(res);
        }
        return Promise.resolve(res);
      })
      .then(function(res) {
        if (p.ipfs.name && res._json.multihash) {
          console.log('publishing index', res._json.multihash);
          p.ipfs.name.publish(res._json.multihash, {}).then(function(res) {
            console.log('published index', res);
          });
        }
        return indexRoot;
      })
      .catch(function(e) {
        console.log('error publishing index', e);
      });
    },

    getIdentityProfileIndexKeys: function(identityProfile, hash) {
      var indexKeys = [];
      var attrs = identityProfile.attrs;
      for (var j = 0; j < attrs.length; j++) {
        var distance = parseInt(attrs[j].dist);
        distance = isNaN(distance) ? 99 : distance;
        distance = ('00'+distance).substring(distance.toString().length); // pad with zeros
        var value = encodeURIComponent(attrs[j].val);
        var lowerCaseValue = encodeURIComponent(attrs[j].val.toLowerCase());
        var name = encodeURIComponent(attrs[j].name);
        var key = distance + ':' + value + ':' + name + ':' + hash.substr(0, 9);
        var lowerCaseKey = distance + ':' + lowerCaseValue + ':' + name + ':' + hash.substr(0, 9);
        indexKeys.push(key);
        if (key !== lowerCaseKey) {
          indexKeys.push(lowerCaseKey);
        }
        if (attrs[j].val.indexOf(' ') > -1) {
          var words = attrs[j].val.toLowerCase().split(' ');
          for (var l = 0; l < words.length; l++) {
            var k = distance + ':' + encodeURIComponent(words[l]) + ':' + name + ':' + hash.substr(0, 9);
            indexKeys.push(k);
          }
        }
        if (key.match(/^http(s)?:\/\/.+\/[a-zA-Z0-9_]+$/)) {
          var split = key.split('/');
          indexKeys.push(split[split.length - 1]);
        }
      }
      return indexKeys;
    },

    getIdentityProfile: function(attrs) {
      var identityProfile = { attrs: attrs };
      if (!attrs.length) {
        return [];
      }
      var d1 = new Date();
      return pub.getMessages({
        recipient: [attrs[0].name, attrs[0].val], // TODO: make sure this attr is unique
        limit: 10000,
        orderBy: 'timestamp',
        direction: 'asc',
        viewpoint: myId
      })
      .then(function(received) {
        //console.log('getMessages by recipient took', d1 - new Date(), 'ms');
        var msgs = [];
        received.forEach(function(msg) {
          msgs.push({
            key: Date.parse(msg.timestamp) + ':' + (msg.ipfs_hash || msg.hash).substr(0,9),
            value: msg, // TODO: save only ipfs_hash
            targetHash: null
          });
        });
        d1 = new Date();
        return btree.MerkleBTree.fromSortedList(msgs, ipfsIndexWidth, p.ipfsStorage);
      })
      .then(function(receivedIndex) {
        //console.log('recipient msgs btree building took', d1 - new Date(), 'ms'); d1 = new Date();
        identityProfile.received = receivedIndex.rootNode.hash;
        return pub.getMessages({
          author: [attrs[0].name, attrs[0].val], // TODO: make sure this attr is unique
          limit: 10000,
          orderBy: 'timestamp',
          direction: 'asc',
          viewpoint: myId
        });
      })
      .then(function(sent) {
        //console.log('getMessages by author took', d1 - new Date(), 'ms');
        var msgs = [];
        sent.forEach(function(msg) {
          msgs.push({
            key: Date.parse(msg.timestamp) + ':' + (msg.ipfs_hash || msg.hash).substr(0,9),
            value: { jws: msg.jws },
            targetHash: null
          });
        });
        d1 = new Date();
        return btree.MerkleBTree.fromSortedList(msgs, ipfsIndexWidth, p.ipfsStorage);
      })
      .then(function(sentIndex) {
        //console.log('author msgs btree building took', d1 - new Date(), 'ms'); d1 = new Date();
        identityProfile.sent = sentIndex.rootNode.hash;
        return identityProfile;
      })
      .catch(function(e) { console.log('adding', attrs, 'failed:', e); });
    },

    addIdentityToIpfsIndex: function(attrs) {
      var ip;
      return pub.getIdentityProfile(attrs)
      .then(function(identityProfile) {
        ip = identityProfile;
        //console.log('adding identityProfile', identityProfile);
        return p.ipfs.files.add(new Buffer(JSON.stringify(identityProfile), 'utf8'));
      })
      .then(function(res) {
        if (res.length) {
          var hash = crypto.createHash('md5').update(JSON.stringify(ip)).digest('base64');
          var q = Promise.resolve(), q2 = Promise.resolve();
          pub.getIdentityProfileIndexKeys(ip, hash).forEach(function(key) { // TODO: why this failing?
            //console.log('adding key to index:', key);
            q = q.then(p.ipfsIdentitiesByDistance.put(key, res[0].hash));
            q2 = q2.then(p.ipfsIdentitiesBySearchKey.put(key.substr(key.indexOf(':') + 1), res[0].hash));
          });
          return timeoutPromise(Promise.all([q, q2]), 30000);
        }
      });
    },

    addIdentityIndexToIpfs: function() {
      var maxIndexSize = 100000;
      var identityIndexEntriesToAdd = [];
      var identityProfilesByHash = {};
      return this.getIdentityAttributes({ limit: maxIndexSize })
      .then(function(res) {
        console.log('adding to ipfs', res.length);
        function iterate(i) {
          console.log(i + '/' + res.length);
          if (i >= res.length) {
            return;
          }
          return pub.getIdentityProfile(res[i])
          .then(function(identityProfile) {
            var hash = crypto.createHash('md5').update(JSON.stringify(identityProfile)).digest('base64');
            identityProfilesByHash[hash] = identityProfile;
            pub.getIdentityProfileIndexKeys(identityProfile, hash).forEach(function(key) {
              identityIndexEntriesToAdd.push({ key: key, value: hash, targetHash: null });
            });
            return iterate(i + 1);
          });
        }
        return iterate(0)
        .then(function() {
          var orderedKeys = Object.keys(identityProfilesByHash);
          function addIdentityProfilesToIpfs() {
            if (!orderedKeys.length) {
              return;
            }
            var keys = orderedKeys.splice(0, 100);
            var values = [];
            keys.forEach(function(key) {
              values.push(new Buffer(JSON.stringify(identityProfilesByHash[key]), 'utf8'));
            });
            return p.ipfs.files.add(values)
            .then(function(res) {
              keys.forEach(function(key, i) {
                if (i < res.length && res[i].hash) {
                  identityProfilesByHash[key] = res[i].hash;
                }
              });
              return addIdentityProfilesToIpfs();
            });
          }
          return addIdentityProfilesToIpfs();
        })
        .then(function() {
          identityIndexEntriesToAdd.forEach(function(entry) {
            entry.value = identityProfilesByHash[entry.value];
          });
          console.log('building index identities_by_distance');
          return btree.MerkleBTree.fromSortedList(identityIndexEntriesToAdd.sort(sortByKey).slice(), ipfsIndexWidth, p.ipfsStorage);
        })
        .then(function(index) {
          p.ipfsIdentitiesByDistance = index;
          identityIndexEntriesToAdd.forEach(function(entry) {
            entry.key = entry.key.substr(entry.key.indexOf(':') + 1);
          });
          console.log('building index identities_by_searchkey');
          return btree.MerkleBTree.fromSortedList(identityIndexEntriesToAdd.sort(sortByKey), ipfsIndexWidth, p.ipfsStorage);
        })
        .then(function(index) {
          p.ipfsIdentitiesBySearchKey = index;
        });
      })
      .then(function() {
        return pub.addIndexRootToIpfs();
      });
    },

    getMsgIndexKey: function(msg) {
      var distance = parseInt(msg.distance);
      distance = isNaN(distance) ? 99 : distance;
      distance = ('00'+distance).substring(distance.toString().length); // pad with zeros
      var key = distance + ':' + Math.floor(Date.parse(msg.timestamp || msg.signedData.timestamp) / 1000) + ':' + (msg.ipfs_hash || msg.hash).substr(0,9);
      return key;
    },

    addMessageIndexToIpfs: function() {
      var limit = 10000;
      var offset = 0;
      var distance = 0;
      var maxMsgCount = 100000;
      var maxDepth = 10;
      var msgsToIndex = [];
      function iterate(limit, offset, distance) {
        return pub.getMessages({
          limit: limit,
          offset: offset,
          orderBy: 'timestamp',
          direction: 'asc',
          viewpoint: myId,
          where: { 'td.distance': distance }
        })
        .then(function(msgs) {
          if (msgs.length === 0 && offset === 0) {
            return msgs.length;
          }
          if (msgs.length < limit) {
            distance += 1;
            offset = 0;
          } else {
            offset += limit;
          }
          msgs.forEach(function(msg) {
            process.stdout.write(".");
            msg = Message.decode(msg);
            msg.distance = distance;
            var key = pub.getMsgIndexKey(msg);
            msgsToIndex.push({ key: key, value: msg, targetHash: null });
          });
          return msgs.length;
        })
        .then(function(msgsLength) {
          var hasMore = !(msgsLength === 0 && offset === 0);
          if (msgsToIndex.length < maxMsgCount && hasMore) {
            return iterate(limit, offset, distance);
          }
          return msgsToIndex.length;
        });
      }

      console.log('adding msgs to ipfs');
      return iterate(limit, offset, distance)
      .then(function(res) {
        console.log('res', res);
        console.log('adding messages_by_distance index to ipfs');
        return btree.MerkleBTree.fromSortedList(msgsToIndex.slice(), ipfsIndexWidth, p.ipfsStorage);
      })
      .then(function(index) {
        p.ipfsMessagesByDistance = index;

        // create index of messages sorted by timestamp
        msgsToIndex.forEach(function(msg) {
          msg.key = msg.key.substr(msg.key.indexOf(':') + 1);
        });
        msgsToIndex = msgsToIndex.sort(sortByKey);
        console.log('adding messages_by_timestamp index to ipfs');
        return btree.MerkleBTree.fromSortedList(msgsToIndex, ipfsIndexWidth, p.ipfsStorage);
      })
      .then(function(index) {
        p.ipfsMessagesByTimestamp = index;
        // Add message index to IPFS
        return pub.addIndexRootToIpfs();
      });
    },

    saveMessageFromIpfs: function(path) {
      return knex('Messages').where('ipfs_hash', path).count('* as count')
      .then(function(res) {
        if (parseInt(res[0].count) === 0) {
          return timeoutPromise(p.ipfs.files.cat(path, { buffer: true }), 5000)
          .then(function(buffer) {
            if (buffer) {
              var msg = { jws: buffer.toString('utf8'), ipfs_hash: path };
              process.stdout.write("+");
              Message.verify(msg);
              console.log('saving new msg from ipfs:', msg.ipfs_hash);
              return pub.saveMessage(msg);
            } else {
              process.stdout.write("-");
            }
          })
          .catch(function(e) {
            console.log('Processing message', path, 'failed:', e);
          });
        }
      });
    },

    saveMessagesFromIpfsIndex: function(ipnsName) {
      console.log('Getting path for name', ipnsName);
      if (!(p.ipfs && p.ipfs.name)) {
        console.log('ipfs.name is not available');
        return;
      }
      var getName = timeoutPromise(p.ipfs.name.resolve(ipnsName), 30000);
      return getName
      .then(function(res) {
        if (!res) { throw new Error('Ipfs index name was not resolved', ipnsName); }
        var path = res['Path'].replace('/ipfs/', '');
        console.log('resolved name', path);
        return timeoutPromise(p.ipfs.object.links(path), 30000);
      })
      .then(function(links) {
        if (!links) { throw new Error('Ipfs index was not resolved', ipnsName); }
        var path;
        for (var i = 0; i < links.length; i++) {
          if (links[i]._name === 'messages_by_distance') {
            path = links[i]._multihash;
          }
        }
        if (!path) {
          throw new Error('No messages index found at', ipnsName);
        }
        console.log('Looking up index');
        return btree.MerkleBTree.getByHash(path, p.ipfsStorage, ipfsIndexWidth);
      })
      .then(function(index) {
        return index.searchText('', 100000);
      })
      .then(function(msgs) {
        var i;
        var q = Promise.resolve();
        console.log('Processing', msgs.length, 'messages from index');
        msgs.forEach(function(entry) {
          var msg = { jws: entry.value.jws };
          if (Message.decode(msg)) {
            process.stdout.write(".");
            q = q.then(function() {
              return pub.saveMessage(msg, false, false);
            });
          }
        });
        return q;
      })
      .then(function() {
        console.log('Finished saving messages from index', ipnsName);
        return pub.addDbMessagesToIpfs();
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
        var q = Promise.resolve();
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
          return Promise.resolve(!!parseInt(res[0].exists));
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

      var authorIdentityIdQuery = Promise.resolve([]),
        recipientIdentityIdQuery = Promise.resolve([]);
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

      return Promise.all([authorIdentityIdQuery, recipientIdentityIdQuery]).then(function(response) {
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

        if (options.savedAtGt) {
          query.andWhere('Messages.saved_at', '>', options.savedAtGt);
        }

        if (options.savedAtLt) {
          query.andWhere('Messages.saved_at', '<', options.savedAtLt);
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

      if (options.id) {
        options.where['attr.name'] = options.id[0];
        options.where['attr.value'] = options.id[1];
      }

      options.where['attr.viewpoint_name'] = options.viewpoint[0];
      options.where['attr.viewpoint_value'] = options.viewpoint[1];

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

      var q = knex.from('IdentityAttributes AS attr')
        .select([
          'attr.identity_id',
          'attr.name',
          'attr.value',
          'attr.confirmations',
          'attr.refutations',
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

      return q = q.then(function(res) {
        var identities = {};
        var i;
        for (i = 0; i < res.length; i++) {
          var attr = res[i];
          identities[attr.identity_id] = identities[attr.identity_id] || [];
          identities[attr.identity_id].push({
            name: attr.name,
            val: attr.value,
            dist: attr.distance,
            pos: attr.positive_score,
            neg: attr.negative_score,
            conf: attr.confirmations,
            ref: attr.refutations
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

        return Promise.resolve(arr);
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
              return Promise.resolve(existingId);
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
                .select('name', 'value as val', 'confirmations as conf', 'refutations as ref')
                .where('identity_id', identityId)
                .whereIn('name', options.searchedAttributes)
                .orderByRaw('confirmations - refutations DESC');
            }

            return knex('IdentityAttributes')
              .select('name', 'value as val', 'confirmations as conf', 'refutations as ref')
              .where('identity_id', identityId)
              .orderByRaw('confirmations - refutations DESC');
          });
      });
    },

    getTrustDistance: function(from, to) {
      if (from[0] === to[0] && from[1] === to[1]) {
        return Promise.resolve(0);
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
        return Promise.resolve(distance);
      });
    },

    getTrustDistances: function(from) {
      return knex.select('*').from('TrustDistances').where({
        'start_attr_name': from[0],
        'start_attr_value': from[1]
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
        q = Promise.resolve();
      }
      q = q.then(function() {
        return pub.mapIdentityAttributes({ id: id, viewpoint: id });
      });
      var i;
      return q = q.then(function() {
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
              q2 = Promise.resolve();
              for (i = 1; i <= maxDepth; i++) {
                q2.then(buildQuery(true, trx, i));
              }
              return q2;
            })
            .then(function() {
              q2 = Promise.resolve();
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
        return Promise.resolve(pub.trustIndexedAttributes);
      }
      return knex('TrustIndexedAttributes').select('*').then(function(res) {
        pub.trustIndexedAttributes = res;
        return res;
      });
    },

    getTrustPaths: function(start, end, maxLength, shortestOnly, viewpoint, limit) {
      return Promise.resolve([]); // Disabled until the performance is improved

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

      var identityId, identityIdQuery = Promise.resolve();
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

        return Promise.all([sent, received]).then(function(response) {
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

          return Promise.resolve(res);
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
          return Promise.all(queries);
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
          return Promise.resolve(0);
        }
        var i, queries = [];
        // Get distances to message authors
        for (i = 0; i < message.signedData.author.length; i++) {
          var q = pub.getTrustDistance(myId, message.signedData.author[i]);
          queries.push(q);
        }

        return Promise.all(queries).then(function(authorDistances) {
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
          return Promise.resolve(priority);
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
        return Promise.resolve();
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
        return Promise.all(queries);
      });

      // TODO:
      // Get TrustIndexedAttributes as t
      // Return unless message signer and author are trusted by t
      // Find existing or new identity_id for message recipient UniqueIdentifierTypes
      // If the attribute exists on the identity_id, increase confirmations or refutations
      // If the attribute doesn't exist, add it with 1 confirmation or refutation
    },

    deletePreviousMessage: function(message, deleteFromIpfsIndexes) {
      var i, j;
      var verifyTypes = ['verify_identity', 'unverify_identity'], t = message.signedData.type;
      var isVerifyMsg = verifyTypes.indexOf(t) > -1;

      function getHashesQuery(author, recipient) {
        var q = knex('Messages as m')
          .distinct('m.hash as hash', 'm.ipfs_hash as ipfs_hash', 'm.timestamp as timestamp', 'td.distance as distance')
          .innerJoin('MessageAttributes as author', function() {
            this.on('author.message_hash', '=', 'm.hash');
            this.andOn('author.is_recipient', '=', knex.raw('?', false));
          })
          .innerJoin('MessageAttributes as recipient', function() {
            this.on('recipient.message_hash', '=', 'm.hash');
            this.andOn('recipient.is_recipient', '=', knex.raw('?', true));
          })
          .innerJoin('UniqueIdentifierTypes as ia1', 'ia1.name', 'author.name')
          .leftJoin('TrustDistances as td', function() {
            this.on('td.start_attr_name', '=', knex.raw('?', myId[0]));
            this.andOn('td.start_attr_value', '=', knex.raw('?', myId[1]));
            this.andOn('td.end_attr_name', '=', 'author.name');
            this.andOn('td.end_attr_value', '=', 'author.value');
          })
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
      var ipfsIndexKeys = [];

      function addHashes(res) {
        for (var i = 0; i < res.length; i++) {
          hashes.push(res[i].hash);
          if (deleteFromIpfsIndexes && res[i].ipfs_hash && res[i].ipfs_hash.length) {
            var msg = res[i];
            ipfsIndexKeys.push(pub.getMsgIndexKey(msg));
          }
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
          queries.push(q = q.then(addHashes));
        }
      }

      // Delete possible previous msg from A->B (created less than minMessageInterval ago?)
      for (i = 0; i < message.signedData.author.length; i++) {
        for (j = 0; j < message.signedData.recipient.length; j++) {
          queries.push(getAndAddHashes(message.signedData.author[i],  message.signedData.recipient[j]));
        }
      }

      return Promise.all(queries).then(function() {
        queries = [];
        hashes = util.removeDuplicates(hashes);
        for (i = 0; i < hashes.length; i++) {
          queries.push(pub.dropMessage(hashes[i]));
        }
        ipfsIndexKeys = util.removeDuplicates(ipfsIndexKeys);
        for (i = 0; i < ipfsIndexKeys.length; i++) {
          console.log('deleting from index', ipfsIndexKeys[i], ipfsIndexKeys[i].substr(ipfsIndexKeys[i].indexOf(':') + 1));
          var shit = ipfsIndexKeys[i].substr(ipfsIndexKeys[i].indexOf(':') + 1);
          queries.push(p.ipfsMessagesByDistance.delete(ipfsIndexKeys[i]));
          queries.push(p.ipfsMessagesByTimestamp.delete(ipfsIndexKeys[i].substr(ipfsIndexKeys[i].indexOf(':') + 1)));
        }
        return Promise.all(queries);
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

      return Promise.all(queries);
    },

    getIndexesFromIpfsRoot: function() {
      return knex('TrustIndexedAttributes')
        .where({ name: myId[0], value: myId[1] })
        .whereNotNull('ipfs_index_root')
        .select('ipfs_index_root')
      .then(function(res) {
        var q = Promise.resolve();
        if (res.length) {
          q = q.then(function() {
            return p.ipfs.object.links(res[0].ipfs_index_root)
            .then(function(links) {
              var queries = [];
              for (var i = 0; i < links.length; i++) {
                switch(links[i]._name) {
                  case 'messages_by_distance':
                    queries.push(btree.MerkleBTree.getByHash(links[i]._multihash, p.ipfsStorage).then(function(index) {
                      p.ipfsMessagesByDistance = index;
                    }));
                    break;
                  case 'messages_by_timestamp':
                    queries.push(btree.MerkleBTree.getByHash(links[i]._multihash, p.ipfsStorage).then(function(index) {
                      p.ipfsMessagesByTimestamp = index;
                    }));
                    break;
                  case 'identities_by_distance':
                    queries.push(btree.MerkleBTree.getByHash(links[i]._multihash, p.ipfsStorage).then(function(index) {
                      p.ipfsIdentitiesByDistance = index;
                    }));
                    break;
                  case 'identities_by_searchkey':
                    queries.push(btree.MerkleBTree.getByHash(links[i]._multihash, p.ipfsStorage).then(function(index) {
                      p.ipfsIdentitiesBySearchKey = index;
                    }));
                    break;
                }
              }
              return Promise.all(queries);
            });
          });
        }
        q = timeoutPromise(q, 15000);
        return q = q.then(function() {
          p.ipfsIdentitiesBySearchKey = p.ipfsIdentitiesBySearchKey || new btree.MerkleBTree(p.ipfsStorage, 100);
          p.ipfsIdentitiesByDistance = p.ipfsIdentitiesByDistance || new btree.MerkleBTree(p.ipfsStorage, 100);
          p.ipfsMessagesByDistance = p.ipfsMessagesByDistance || new btree.MerkleBTree(p.ipfsStorage, 100);
          p.ipfsMessagesByTimestamp = p.ipfsMessagesByTimestamp || new btree.MerkleBTree(p.ipfsStorage, 100);
        });
      });
    }
  };

  pub.init = function(conf, ipfs) {
    p.ipfs = ipfs;
    p.ipfsStorage = new btree.IPFSStorage(p.ipfs);
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
        return p.getIndexesFromIpfsRoot();
      })
      .then(function() {
        if (process.env.NODE_ENV !== 'test') {
          pub.saveMessagesFromIpfsIndexes(); // non-blocking
        }
        pub.keepAddingNewMessagesToIpfsIndex();
      });
  };

  return pub;
};
