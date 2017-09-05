
const Promise = require('bluebird');
const util = require('./util.js');
const schema = require('./schema.js');

const crypto = require('crypto');
const btree = require('merkle-btree');

const Message = require('identifi-lib/message');
const keyutil = require('identifi-lib/keyutil');

const myKey = keyutil.getDefault();
const myId = ['keyID', myKey.hash];
const myTrustIndexDepth = 4;
let config;
const ipfsIndexWidth = 200;

const dagPB = require('ipld-dag-pb');

let SQL_IFNULL = 'IFNULL';

const REBUILD_INDEXES_IF_NEW_MSGS_GT = 30;

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
    new Promise(((resolve) => {
      setTimeout(() => {
        // console.log('promise timed out');
        resolve();
      }, timeout);
    })),
  ]);
}

module.exports = function (knex) {
  let p; // Private methods

  let lastIpfsIndexedMessageSavedAt = (new Date()).toISOString();

  let ipfsIdentityIndexKeysToRemove = {};

  const pub = {
    trustIndexedAttributes: null,
    saveMessage(message, updateTrustIndexes, addToIpfs) {
      if (typeof updateTrustIndexes === 'undefined') { updateTrustIndexes = true; }
      if (typeof addToIpfs === 'undefined') { addToIpfs = true; }
      if (typeof message.signerKeyHash === 'undefined') {
        message.signerKeyHash = Message.getSignerKeyHash(message);
      }
      const q = this.ensureFreeSpace();
      // Unobtrusively store msg to ipfs
      addToIpfs = (p.ipfs && !message.ipfs_hash) && addToIpfs;
      if (addToIpfs) {
        q.then(() => pub.addMessageToIpfs(message))
          .then((res) => {
            message.ipfs_hash = res[0].hash;
          });
      }
      return q.then(() => pub.messageExists(message.hash))
        .then((exists) => {
          if (exists) {
            if (addToIpfs && message.ipfs_hash) {
            // Msg was added to IPFS - update ipfs_hash
              return knex('Messages').where({ hash: message.hash }).update({ ipfs_hash: message.ipfs_hash });
            }
            return Promise.resolve(false);
          }
          const q2 = Promise.resolve();
          if (Object.keys(ipfsIdentityIndexKeysToRemove).length < REBUILD_INDEXES_IF_NEW_MSGS_GT) {
            // Mark for deletion the index references to expiring identity profiles
            ipfsIdentityIndexKeysToRemove[message.hash] = [];
            q2.then(() => pub.getIdentityAttributesByAuthorOrRecipient(message, true))
              .then(attrs => pub.getIndexKeysByIdentity(attrs))
              .then((keys) => {
                ipfsIdentityIndexKeysToRemove[message.hash] = ipfsIdentityIndexKeysToRemove[message.hash].concat(keys);
                return pub.getIdentityAttributesByAuthorOrRecipient(message, false);
              })
              .then(attrs => pub.getIndexKeysByIdentity(attrs))
              .then((keys) => {
                ipfsIdentityIndexKeysToRemove[message.hash] = ipfsIdentityIndexKeysToRemove[message.hash].concat(keys);
              });
          }
          const isPublic = typeof message.signedData.public === 'undefined' ? true : message.signedData.public;
          return q2.then(() => p.deletePreviousMessage(message))
            .then(() => p.getPriority(message))
            .then(priority => knex.transaction(trx => trx('Messages').insert({
              hash: message.hash,
              ipfs_hash: message.ipfs_hash,
              jws: message.jws,
              timestamp: message.signedData.timestamp,
              type: message.signedData.type || 'rating',
              rating: message.signedData.rating || 0,
              max_rating: message.signedData.maxRating || 0,
              min_rating: message.signedData.minRating || 0,
              public: isPublic,
              priority,
              is_latest: p.isLatest(message),
              signer_keyid: message.signerKeyHash,
              saved_at: new Date().toISOString(),
            })
              .then(() => {
                let i,
                  queries = [];
                for (i = 0; i < message.signedData.author.length; i += 1) {
                  queries.push(trx('MessageAttributes').insert({
                    message_hash: message.hash,
                    name: message.signedData.author[i][0],
                    value: message.signedData.author[i][1],
                    is_recipient: false,
                  }));
                }
                for (i = 0; i < message.signedData.recipient.length; i += 1) {
                  queries.push(trx('MessageAttributes').insert({
                    message_hash: message.hash,
                    name: message.signedData.recipient[i][0],
                    value: message.signedData.recipient[i][1],
                    is_recipient: true,
                  }));
                }
                return Promise.all(queries);
              }))
              .then(() => {
                if (updateTrustIndexes) {
                  return p.updateWotIndexesByMessage(message)
                    .then(() => p.updateIdentityIndexesByMessage(message)
                      .then(res => res));
                }
              }));
        })
        .then(() => message);
    },

    keepAddingNewMessagesToIpfsIndex() {
      return pub.addNewMessagesToIpfsIndex()
        .then(() => new Promise(((resolve) => {
          setTimeout(() => {
            resolve(pub.keepAddingNewMessagesToIpfsIndex());
          }, 10000);
        })));
    },

    addIndexesToIpfs() {
      return pub.addMessageIndexToIpfs()
        .then(() => pub.addIdentityIndexToIpfs());
    },

    addNewMessagesToIpfsIndex() {
      let msgCount;
      return pub.getMessages({
        limit: 10000,
        orderBy: 'timestamp',
        direction: 'desc',
        viewpoint: myId,
        savedAtGt: lastIpfsIndexedMessageSavedAt,
      })
        .then((messages) => {
          msgCount = messages.length;
          if (messages.length) {
            console.log('', messages.length, 'new messages to index');
          }
          // rebuilding the indexes is more efficient than inserting large number of entries individually
          if (messages.length < REBUILD_INDEXES_IF_NEW_MSGS_GT) {
            let q = Promise.resolve();
            // remove identity index entries that point to expired identity profiles
            Object.keys(ipfsIdentityIndexKeysToRemove).forEach((msg) => {
              ipfsIdentityIndexKeysToRemove[msg].forEach((key) => {
                q.then(() => {
                  const q2 = p.ipfsIdentitiesByDistance.delete(key);
                  const q3 = p.ipfsIdentitiesBySearchKey.delete(key.substr(key.indexOf(':') + 1));
                  return timeoutPromise(Promise.all([q2, q3]), 30000);
                });
                delete ipfsIdentityIndexKeysToRemove[msg];
              });
            });
            messages.forEach((message) => {
              message = Message.decode(message);
              const d = new Date(message.saved_at).toISOString();
              if (d > lastIpfsIndexedMessageSavedAt) {
                lastIpfsIndexedMessageSavedAt = d;
              }
              q = q.then(() => pub.addMessageToIpfsIndex(message));
            });
            return timeoutPromise(q.return(messages.length), 100000)
              .then((res) => {
                if (typeof res === undefined) { return pub.addIndexesToIpfs(); }
                return res;
              });
          }
          return pub.addIndexesToIpfs()
            .then(() => {
              ipfsIdentityIndexKeysToRemove = {};
            });
        })
        .then(() => {
          if (msgCount) {
            console.log('adding index root to ipfs');
            return pub.addIndexRootToIpfs();
          }
        })
        .catch((e) => { console.log('adding new messages to ipfs failed:', e); });
    },

    getIdentityAttributesByAuthorOrRecipient(message, getByAuthor, limit) {
      // pick first unique author or recipient attribute from message
      const attributes = getByAuthor ? message.signedData.author : message.signedData.recipient;
      for (let i = 0; i < attributes.length; i += 1) {
        if (pub.isUniqueType(attributes[i][0])) {
          return pub.getIdentityAttributes({ limit: limit || 10, id: attributes[i] })
            .then(attrs => (attrs.length ? attrs[0] : []));
        }
      }
      return [];
    },

    addMessageToIpfsIndex(message) {
      const msgIndexKey = pub.getMsgIndexKey(message); // TODO: should have distance
      return p.ipfsMessagesByDistance.put(msgIndexKey, message)
        .then(() => p.ipfsMessagesByTimestamp.put(msgIndexKey.substr(msgIndexKey.indexOf(':') + 1), message))
        .then(() => pub.getIdentityAttributesByAuthorOrRecipient(message, true))
        .then(authorAttrs => pub.addIdentityToIpfsIndex(authorAttrs))
        .then(() => pub.getIdentityAttributesByAuthorOrRecipient(message, false))
        .then((recipientAttrs) => {
          let shortestDistance = 99;
          recipientAttrs.forEach((attr) => {
            if (typeof attr.dist === 'number' && attr.dist < shortestDistance) {
              shortestDistance = attr.dist;
            }
          });
          if (shortestDistance < 99) {
            message.distance = shortestDistance;
          }
          return pub.addIdentityToIpfsIndex(recipientAttrs);
        })
        .catch((e) => { console.log('adding to ipfs failed:', e); });
    },

    addMessageToIpfs(message) {
      return p.ipfs.files.add(Buffer.from(message.jws, 'utf8'));
    },

    addDbMessagesToIpfs() {
      let counter = 0;

      function getReindexQuery() {
        const msgs = {};
        return knex('Messages').whereNull('ipfs_hash').select('jws', 'hash').limit(100)
          .then((res) => {
            if (res.length && p.ipfs) {
              res.forEach((msg) => {
                msgs[msg.hash] = Buffer.from(msg.jws, 'utf8');
              });
              return p.ipfs.files.add(Object.values(msgs));
            }
            return [];
          })
          .then((res) => {
            console.log('added', res.length, 'msgs to ipfs');
            const queries = [];
            Object.keys(msgs).forEach((hash, index) => {
              queries.push(knex('Messages').where({ hash }).update({ ipfs_hash: res[index].hash }).return());
            });
            return Promise.all(queries);
          })
          .then((res) => {
            console.log('updated', res.length, 'db entries');
            if (res.length) {
              counter += 1;
              return getReindexQuery();
            }
          })
          .catch((e) => { console.log('adding to ipfs failed:', e); });
      }

      return getReindexQuery().then(() => `Reindexed ${counter} messages`);
    },

    addIndexRootToIpfs() {
      // saves indexes as an IPFS directory
      // TODO: if info already exists, don't rewrite
      let indexRoot;
      return pub.getIdentityAttributes({ id: myId })
        .then((attrs) => {
          if (attrs.length) {
            attrs = attrs[0];
          } else {
            attrs = [];
          }
          return p.ipfs.files.add([
            { path: 'info', content: Buffer.from(JSON.stringify({ keyID: myId[1], attrs })) },
            { path: 'messages_by_distance', content: Buffer.from(p.ipfsMessagesByDistance.rootNode.serialize()) },
            { path: 'messages_by_timestamp', content: Buffer.from(p.ipfsMessagesByTimestamp.rootNode.serialize()) },
            { path: 'identities_by_searchkey', content: Buffer.from(p.ipfsIdentitiesBySearchKey.rootNode.serialize()) },
            { path: 'identities_by_distance', content: Buffer.from(p.ipfsIdentitiesByDistance.rootNode.serialize()) },
          ]);
        })
        .then((res) => {
          const links = [];
          for (let i = 0; i < res.length; i += 1) {
            links.push({ Name: res[i].path, Hash: res[i].hash, Size: res[i].size });
          }
          return new Promise(((resolve, reject) => {
            dagPB.DAGNode.create(Buffer.from('\u0008\u0001'), links, (err, dag) => {
              if (err) {
                reject(err);
              }
              resolve(p.ipfs.object.put(dag));
            });
          }));
        })
        .then((res) => {
          if (res._json.multihash) {
            indexRoot = res._json.multihash;
            return knex('TrustIndexedAttributes')
              .where({ name: myId[0], value: myId[1] })
              .update('ipfs_index_root', res._json.multihash)
              .return(res);
          }
          return Promise.resolve(res);
        })
        .then((res) => {
          if (p.ipfs.name && res._json.multihash) {
            console.log('publishing index', res._json.multihash);
            p.ipfs.name.publish(res._json.multihash, {}).then((res) => {
              console.log('published index', res);
            });
          }
          return indexRoot;
        })
        .catch(e => console.log('error publishing index', e));
    },

    getIdentityProfileIndexKeys(identityProfile, hash) {
      const indexKeys = [];
      const attrs = identityProfile.attrs;
      for (let j = 0; j < attrs.length; j += 1) {
        let distance = parseInt(attrs[j].dist);
        distance = isNaN(distance) ? 99 : distance;
        distance = (`00${distance}`).substring(distance.toString().length); // pad with zeros
        const value = encodeURIComponent(attrs[j].val);
        const lowerCaseValue = encodeURIComponent(attrs[j].val.toLowerCase());
        const name = encodeURIComponent(attrs[j].name);
        const key = `${distance}:${value}:${name}`;
        const lowerCaseKey = `${distance}:${lowerCaseValue}:${name}`;
        // TODO: add  + ':' + hash.substr(0, 9) to non-unique identifiers, to allow for duplicates
        indexKeys.push(key);
        if (key !== lowerCaseKey) {
          indexKeys.push(lowerCaseKey);
        }
        if (attrs[j].val.indexOf(' ') > -1) {
          const words = attrs[j].val.toLowerCase().split(' ');
          for (let l = 0; l < words.length; l++) {
            const k = `${distance}:${encodeURIComponent(words[l])}:${name}:${hash.substr(0, 9)}`;
            indexKeys.push(k);
          }
        }
        if (key.match(/^http(s)?:\/\/.+\/[a-zA-Z0-9_]+$/)) {
          const split = key.split('/');
          indexKeys.push(split[split.length - 1]);
        }
      }
      return indexKeys;
    },

    getIdentityProfile(attrs) {
      const identityProfile = { attrs };
      if (!attrs.length) {
        return Promise.resolve(identityProfile);
      }
      let d1 = new Date();
      let uniqueAttr = attrs[0];
      for (let i = 0; i < attrs.length; i += 1) {
        if (pub.isUniqueType(attrs[i].name)) {
          uniqueAttr = attrs[i];
        }
      }

      return pub.getMessages({
        recipient: [uniqueAttr.name, uniqueAttr.val],
        limit: 10000,
        orderBy: 'timestamp',
        direction: 'asc',
        viewpoint: myId,
      })
        .then((received) => {
        // console.log('getMessages by recipient took', d1 - new Date(), 'ms');
          const msgs = [];
          received.forEach((msg) => {
            msgs.push({
              key: `${Date.parse(msg.timestamp)}:${(msg.ipfs_hash || msg.hash).substr(0, 9)}`,
              value: msg, // TODO: save only ipfs_hash
              targetHash: null,
            });
          });
          d1 = new Date();
          if (msgs.length && p.ipfsStorage) {
            return btree.MerkleBTree.fromSortedList(msgs, ipfsIndexWidth, p.ipfsStorage);
          }
        })
        .then((receivedIndex) => {
          if (receivedIndex) {
            identityProfile.received = receivedIndex.rootNode.hash;
          }
          // console.log('recipient msgs btree building took', d1 - new Date(), 'ms'); d1 = new Date();
          return pub.getMessages({
            author: [uniqueAttr.name, uniqueAttr.val],
            limit: 10000,
            orderBy: 'timestamp',
            direction: 'asc',
            viewpoint: myId,
          });
        })
        .then((sent) => {
        // console.log('getMessages by author took', d1 - new Date(), 'ms');
          const msgs = [];
          sent.forEach((msg) => {
            msgs.push({
              key: `${Date.parse(msg.timestamp)}:${(msg.ipfs_hash || msg.hash).substr(0, 9)}`,
              value: { jws: msg.jws },
              targetHash: null,
            });
          });
          d1 = new Date();
          if (msgs.length && p.ipfsStorage) {
            return btree.MerkleBTree.fromSortedList(msgs, ipfsIndexWidth, p.ipfsStorage);
          }
        })
        .then((sentIndex) => {
        // console.log('author msgs btree building took', d1 - new Date(), 'ms'); d1 = new Date();
          if (sentIndex) {
            identityProfile.sent = sentIndex.rootNode.hash;
          }
          return identityProfile;
        })
        .catch((e) => {
          console.log('p.ipfs', p.ipfs);
          console.log('adding', attrs, 'failed:', e);
          return identityProfile;
        });
    },

    getIndexKeysByIdentity(attrs) {
      return pub.getIdentityProfile(attrs)
        .then((identityProfile) => {
          const keys = [];
          const hash = crypto.createHash('md5').update(JSON.stringify(identityProfile)).digest('base64');
          pub.getIdentityProfileIndexKeys(identityProfile, hash).forEach((key) => {
            keys.push(key);
            keys.push(key.substr(key.indexOf(':') + 1));
          });
          return keys;
        });
    },

    addIdentityToIpfsIndex(attrs) {
      let ip;
      return pub.getIdentityProfile(attrs)
        .then((identityProfile) => {
          ip = identityProfile;
          console.log('adding identityprofile to ipfs', ip);
          return p.ipfs.files.add(Buffer.from(JSON.stringify(identityProfile), 'utf8'));
        })
        .then((res) => {
          if (res.length) {
            const hash = crypto.createHash('md5').update(JSON.stringify(ip)).digest('base64');
            let q = Promise.resolve(),
              q2 = Promise.resolve();
            pub.getIdentityProfileIndexKeys(ip, hash).forEach((key) => {
              console.log('adding key', key);
              console.log('and', key.substr(key.indexOf(':') + 1));
              q = q.then(p.ipfsIdentitiesByDistance.put(key, res[0].hash));
              q2 = q2.then(p.ipfsIdentitiesBySearchKey.put(key.substr(key.indexOf(':') + 1), res[0].hash));
            });
            return timeoutPromise(Promise.all([q, q2]), 30000);
          }
        });
    },

    addIdentityIndexToIpfs() {
      const maxIndexSize = 100000;
      const identityIndexEntriesToAdd = [];
      const identityProfilesByHash = {};
      return this.getIdentityAttributes({ limit: maxIndexSize })
        .then((res) => {
          console.log('Adding identity index of', res.length, 'entries to ipfs');
          function iterate(i) {
            console.log(`${i}/${res.length}`);
            if (i >= res.length) {
              return;
            }
            return pub.getIdentityProfile(res[i])
              .then((identityProfile) => {
                const hash = crypto.createHash('md5').update(JSON.stringify(identityProfile)).digest('base64');
                identityProfilesByHash[hash] = identityProfile;
                pub.getIdentityProfileIndexKeys(identityProfile, hash).forEach((key) => {
                  identityIndexEntriesToAdd.push({ key, value: hash, targetHash: null });
                });
                return iterate(i + 1);
              });
          }
          return iterate(0)
            .then(() => {
              const orderedKeys = Object.keys(identityProfilesByHash);
              function addIdentityProfilesToIpfs() {
                if (!orderedKeys.length) {
                  return;
                }
                const keys = orderedKeys.splice(0, 100);
                const values = [];
                keys.forEach((key) => {
                  values.push(Buffer.from(JSON.stringify(identityProfilesByHash[key]), 'utf8'));
                });
                return p.ipfs.files.add(values)
                  .then((res) => {
                    keys.forEach((key, i) => {
                      if (i < res.length && res[i].hash) {
                        identityProfilesByHash[key] = res[i].hash;
                      }
                    });
                    return addIdentityProfilesToIpfs();
                  });
              }
              return addIdentityProfilesToIpfs();
            })
            .then(() => {
              identityIndexEntriesToAdd.forEach((entry) => {
                entry.value = identityProfilesByHash[entry.value];
              });
              console.log('building index identities_by_distance');
              return btree.MerkleBTree.fromSortedList(identityIndexEntriesToAdd.sort(sortByKey).slice(), ipfsIndexWidth, p.ipfsStorage);
            })
            .then((index) => {
              p.ipfsIdentitiesByDistance = index;
              identityIndexEntriesToAdd.forEach((entry) => {
                entry.key = entry.key.substr(entry.key.indexOf(':') + 1);
              });
              console.log('building index identities_by_searchkey');
              return btree.MerkleBTree.fromSortedList(identityIndexEntriesToAdd.sort(sortByKey), ipfsIndexWidth, p.ipfsStorage);
            })
            .then((index) => {
              p.ipfsIdentitiesBySearchKey = index;
            });
        })
        .then(() => pub.addIndexRootToIpfs());
    },

    getMsgIndexKey(msg) {
      let distance = parseInt(msg.distance);
      distance = isNaN(distance) ? 99 : distance;
      distance = (`00${distance}`).substring(distance.toString().length); // pad with zeros
      const key = `${distance}:${Math.floor(Date.parse(msg.timestamp || msg.signedData.timestamp) / 1000)}:${(msg.ipfs_hash || msg.hash).substr(0, 9)}`;
      return key;
    },

    addMessageIndexToIpfs() {
      const limit = 10000;
      const offset = 0;
      const distance = 0;
      const maxMsgCount = 100000;
      let msgsToIndex = [];
      function iterate(limit, offset, distance) {
        return pub.getMessages({
          limit,
          offset,
          orderBy: 'timestamp',
          direction: 'asc',
          viewpoint: myId,
          where: { 'td.distance': distance },
        })
          .then((msgs) => {
            if (msgs.length === 0 && offset === 0) {
              return msgs.length;
            }
            if (msgs.length < limit) {
              distance += 1;
              offset = 0;
            } else {
              offset += limit;
            }
            msgs.forEach((msg) => {
              process.stdout.write('.');
              msg = Message.decode(msg);
              msg.distance = distance;
              const key = pub.getMsgIndexKey(msg);
              msgsToIndex.push({ key, value: msg, targetHash: null });
            });
            return msgs.length;
          })
          .then((msgsLength) => {
            const hasMore = !(msgsLength === 0 && offset === 0);
            if (msgsToIndex.length < maxMsgCount && hasMore) {
              return iterate(limit, offset, distance);
            }
            return msgsToIndex.length;
          });
      }

      console.log('adding msgs to ipfs');
      return iterate(limit, offset, distance)
        .then((res) => {
          console.log('res', res);
          console.log('adding messages_by_distance index to ipfs');
          return btree.MerkleBTree.fromSortedList(msgsToIndex.slice(), ipfsIndexWidth, p.ipfsStorage);
        })
        .then((index) => {
          p.ipfsMessagesByDistance = index;

          // create index of messages sorted by timestamp
          msgsToIndex.forEach((msg) => {
            msg.key = msg.key.substr(msg.key.indexOf(':') + 1);
          });
          msgsToIndex = msgsToIndex.sort(sortByKey);
          console.log('adding messages_by_timestamp index to ipfs');
          return btree.MerkleBTree.fromSortedList(msgsToIndex, ipfsIndexWidth, p.ipfsStorage);
        })
        .then((index) => {
          p.ipfsMessagesByTimestamp = index;
          // Add message index to IPFS
          return pub.addIndexRootToIpfs();
        });
    },

    saveMessageFromIpfs(path) {
      return knex('Messages').where('ipfs_hash', path).count('* as count')
        .then((res) => {
          if (parseInt(res[0].count) === 0) {
            return timeoutPromise(p.ipfs.files.cat(path, { buffer: true }), 5000)
              .then((buffer) => {
                if (buffer) {
                  const msg = { jws: buffer.toString('utf8'), ipfs_hash: path };
                  process.stdout.write('+');
                  Message.verify(msg);
                  console.log('saving new msg from ipfs:', msg.ipfs_hash);
                  return pub.saveMessage(msg);
                }
                process.stdout.write('-');
              })
              .catch((e) => {
                console.log('Processing message', path, 'failed:', e);
              });
          }
        });
    },

    saveMessagesFromIpfsIndex(ipnsName) {
      console.log('Getting path for name', ipnsName);
      if (!(p.ipfs && p.ipfs.name)) {
        console.log('ipfs.name is not available');
        return;
      }
      const getName = timeoutPromise(p.ipfs.name.resolve(ipnsName), 60000);
      return getName
        .then((res) => {
          if (!res) { throw new Error('Ipfs index name was not resolved', ipnsName); }
          const path = res.Path.replace('/ipfs/', '');
          console.log('resolved name', path);
          return timeoutPromise(p.ipfs.object.links(path), 30000);
        })
        .then((links) => {
          if (!links) { throw new Error('Ipfs index was not resolved', ipnsName); }
          let path;
          for (let i = 0; i < links.length; i += 1) {
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
        .then(index => index.searchText('', 100000))
        .then((msgs) => {
          let q = Promise.resolve();
          console.log('Processing', msgs.length, 'messages from index');
          msgs.forEach((entry) => {
            const msg = { jws: entry.value.jws };
            if (Message.decode(msg)) {
              process.stdout.write('.');
              q = q.then(() => pub.saveMessage(msg, false, false));
            }
          });
          return q;
        })
        .then(() => {
          console.log('Finished saving messages from index', ipnsName);
          return pub.addDbMessagesToIpfs();
        })
        .catch((e) => {
          console.log('Processing index', ipnsName, 'failed:', e);
        });
    },

    saveMessagesFromIpfsIndexes() {
      return knex('Messages')
        .innerJoin('MessageAttributes as author', (q) => {
          q.on('author.message_hash', '=', 'Messages.hash');
          q.andOn('author.is_recipient', '=', knex.raw('?', false));
        })
        .innerJoin('MessageAttributes as recipient', (q) => {
          q.on('recipient.message_hash', '=', 'Messages.hash');
          q.andOn('recipient.is_recipient', '=', knex.raw('?', true));
        })
        .innerJoin('TrustDistances as td', (q) => {
          q.on('td.start_attr_name', '=', knex.raw('?', myId[0]));
          q.on('td.start_attr_value', '=', knex.raw('?', myId[1]));
          q.on('td.end_attr_name', '=', 'author.name');
          q.on('td.end_attr_value', '=', 'author.value');
        })
        .where('td.distance', '<=', 2)
        .andWhere('recipient.name', 'nodeID')
        .select()
        .distinct('recipient.value')
        .then((res) => {
          let i;
          let q = Promise.resolve();
          for (i = 0; i < res.length; i += 1) {
            q = q.then(pub.saveMessagesFromIpfsIndex(res[i].value));
          }
          return q;
        })
        .catch((e) => {
          console.log('Saving messages from indexes failed:', e);
        });
    },

    messageExists(hash) {
      return knex('Messages').where('hash', hash).count('* as exists')
        .then(res => Promise.resolve(!!parseInt(res[0].exists)));
    },

    getMessages(options) {
      const defaultOptions = {
        orderBy: 'timestamp',
        direction: 'desc',
        limit: 100,
        offset: 0,
        where: {},
      };
      options = options || defaultOptions;
      for (const key in defaultOptions) {
        options[key] = options[key] !== undefined ? options[key] : defaultOptions[key];
      }

      let authorIdentityIdQuery = Promise.resolve([]),
        recipientIdentityIdQuery = Promise.resolve([]);
      if (options.viewpoint) {
        if (options.author) {
          authorIdentityIdQuery = knex('IdentityAttributes')
            .where({
              name: options.author[0],
              value: options.author[1],
              viewpoint_name: options.viewpoint[0],
              viewpoint_value: options.viewpoint[1],
            })
            .select('identity_id');
        }

        if (options.recipient) {
          recipientIdentityIdQuery = knex('IdentityAttributes')
            .where({
              name: options.recipient[0],
              value: options.recipient[1],
              viewpoint_name: options.viewpoint[0],
              viewpoint_value: options.viewpoint[1],
            })
            .select('identity_id');
        }
      }

      return Promise.all([authorIdentityIdQuery, recipientIdentityIdQuery]).then((response) => {
        const authorIdentityId = response[0].length > 0 && response[0][0].identity_id;
        const recipientIdentityId = response[1].length > 0 && response[1][0].identity_id;
        const select = ['Messages.*'];
        if (options.viewpoint) {
          select.push(knex.raw('MIN(td.distance) AS "distance"'));
          select.push(knex.raw('MAX(st.positive_score) AS author_pos'));
          select.push(knex.raw('MAX(st.negative_score) AS author_neg'));
        }
        const query = knex.select(select)
          .groupBy('Messages.hash')
          .from('Messages')
          .innerJoin('MessageAttributes as author', (q) => {
            q.on('Messages.hash', '=', 'author.message_hash');
            q.on('author.is_recipient', '=', knex.raw('?', false));
          })
          .innerJoin('MessageAttributes as recipient', (q) => {
            q.on('Messages.hash', '=', 'recipient.message_hash');
            q.andOn('recipient.is_recipient', '=', knex.raw('?', true));
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
          const ratingType = options.where['Messages.type'].match(/(positive|neutral|negative)$/i)[0];
          options.where['Messages.type'] = 'rating';
          const bindings = { rating: 'Messages.rating', max_rating: 'Messages.max_rating', min_rating: 'Messages.min_rating' };
          switch (ratingType) {
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

        if (options.where.hash) {
          const hash = options.where.hash;
          query.where((q) => {
            q.where('Messages.hash', hash);
            q.orWhere('Messages.ipfs_hash', hash);
          });
          delete options.where.hash;
        }

        if (options.viewpoint) {
          // Replace left joins with subquery for performance?
          query.leftJoin('IdentityAttributes as author_attribute', (q) => {
            q.on('author_attribute.name', '=', 'author.name');
            q.on('author_attribute.value', '=', 'author.value');
            q.on('author_attribute.viewpoint_name', '=', knex.raw('?', options.viewpoint[0]));
            q.on('author_attribute.viewpoint_value', '=', knex.raw('?', options.viewpoint[1]));
          });
          query.leftJoin('IdentityStats as st', 'st.identity_id', 'author_attribute.identity_id');
          query.leftJoin('IdentityAttributes as recipient_attribute', (q) => {
            q.on('recipient_attribute.name', '=', 'recipient.name');
            q.on('recipient_attribute.value', '=', 'recipient.value');
            q.on('recipient_attribute.viewpoint_name', '=', knex.raw('?', options.viewpoint[0]));
            q.on('recipient_attribute.viewpoint_value', '=', knex.raw('?', options.viewpoint[1]));
          });
          query.innerJoin('TrustDistances as td', (q) => {
            q.on('author_attribute.name', '=', 'td.end_attr_name')
              .andOn('author_attribute.value', '=', 'td.end_attr_value')
              .andOn('td.start_attr_name', '=', knex.raw('?', options.viewpoint[0]))
              .andOn('td.start_attr_value', '=', knex.raw('?', options.viewpoint[1]));
            if (options.maxDistance > 0) {
              q.andOn('td.distance', '<=', knex.raw('?', options.maxDistance));
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

    dropMessage(messageHash) {
      return knex.transaction(trx => trx('MessageAttributes').where({ message_hash: messageHash }).del()
        .then(() => trx('Messages').where({ hash: messageHash }).del())
        .then(res => !!res));
    },

    getIdentityAttributes(options) {
      const defaultOptions = {
        orderBy: 'value',
        direction: 'asc',
        limit: 100,
        offset: 0,
        where: {},
        having: {},
        viewpoint: myId,
      };
      options = options || defaultOptions;
      for (const key in defaultOptions) {
        options[key] = options[key] !== undefined ? options[key] : defaultOptions[key];
      }

      if (options.id) {
        options.where['attr.name'] = options.id[0];
        options.where['attr.value'] = options.id[1];
      }

      options.where['attr.viewpoint_name'] = options.viewpoint[0];
      options.where['attr.viewpoint_value'] = options.viewpoint[1];

      const subquery = knex.from('IdentityAttributes AS attr2')
        .select(knex.raw('attr.identity_id'))
        .groupBy('attr.identity_id')
        .innerJoin('IdentityAttributes AS attr', (q) => {
          q.on('attr.identity_id', '=', 'attr2.identity_id');
        })
        .leftJoin('TrustDistances as td', (q) => {
          q.on('td.start_attr_name', '=', 'attr.viewpoint_name');
          q.andOn('td.start_attr_value', '=', 'attr.viewpoint_value');
          q.andOn('td.end_attr_name', '=', 'attr.name');
          q.andOn('td.end_attr_value', '=', 'attr.value');
        })
        .where(options.where)
        .orderBy(knex.raw(`MIN(${SQL_IFNULL}(td.distance, 100000))`), options.direction)
        .limit(options.limit)
        .offset(options.offset);

      if (options.searchValue) {
        subquery.where(knex.raw('lower("attr"."value")'), 'LIKE', `%${options.searchValue.toLowerCase()}%`);
      }

      let q = knex.from('IdentityAttributes AS attr')
        .select([
          'attr.identity_id',
          'attr.name',
          'attr.value',
          'attr.confirmations',
          'attr.refutations',
          'td.distance',
          'st.positive_score',
          'st.negative_score',
        ])
        .leftJoin('TrustDistances as td', (qq) => {
          qq.on('td.start_attr_name', '=', 'attr.viewpoint_name');
          qq.andOn('td.start_attr_value', '=', 'attr.viewpoint_value');
          qq.andOn('td.end_attr_name', '=', 'attr.name');
          qq.andOn('td.end_attr_value', '=', 'attr.value');
        })
        .leftJoin('IdentityStats as st', (qq) => {
          qq.on('attr.identity_id', '=', 'st.identity_id');
        })
        .where('attr.identity_id', 'in', subquery);

      // sql += "ORDER BY iid >= 0 DESC, IFNULL(tp.Distance,1000) ASC, CASE WHEN attrvalue LIKE :query || '%' THEN 0 ELSE 1 END, UID.name IS NOT NULL DESC, attrvalue ASC ";

      return q = q.then((res) => {
        const identities = {};
        let i;
        for (i = 0; i < res.length; i += 1) {
          const attr = res[i];
          identities[attr.identity_id] = identities[attr.identity_id] || [];
          identities[attr.identity_id].push({
            name: attr.name,
            val: attr.value,
            dist: attr.distance,
            pos: attr.positive_score,
            neg: attr.negative_score,
            conf: attr.confirmations,
            ref: attr.refutations,
          });
        }

        let arr = [];
        for (const key in identities) {
          arr.push(identities[key]);
        }

        // Sort by distance
        arr = arr.sort((identity1, identity2) => {
          let smallestDistance1 = 1000,
            smallestDistance2 = 1000;
          for (i = 0; i < identity1.length; i += 1) {
            if (!isNaN(parseInt(identity1[i].dist)) && identity1[i].dist < smallestDistance1) {
              smallestDistance1 = identity1[i].dist;
            }
          }
          for (i = 0; i < identity2.length; i += 1) {
            if (!isNaN(parseInt(identity2[i].dist)) && identity2[i].dist < smallestDistance2) {
              smallestDistance2 = identity2[i].dist;
            }
          }
          return smallestDistance1 - smallestDistance2;
        });

        return Promise.resolve(arr);
      });
    },

    mapIdentityAttributes(options) {
      let identityId;
      options.viewpoint = options.viewpoint || myId;
      // Find out existing identity_id for the identifier
      const getExistingId = knex.from('IdentityAttributes as ia')
        .select('identity_id')
        .where({
          'ia.name': options.id[0],
          'ia.value': options.id[1],
          viewpoint_name: options.viewpoint[0],
          viewpoint_value: options.viewpoint[1],
        })
        .innerJoin('UniqueIdentifierTypes as uidt', 'uidt.name', 'ia.name');
      let existingId;

      return getExistingId.then((res) => {
        existingId = res;
        return knex('IdentityAttributes')
          // Delete previously saved attributes of the identity_id
          .where('identity_id', 'in', getExistingId).del()
          .then(() => {
            if (existingId.length) {
              // Pass on the existing identity_id
              return Promise.resolve(existingId);
            }
            return knex('IdentityAttributes')
              // No existing identity_id - return a new one
              .select(knex.raw(`${SQL_IFNULL}(MAX(identity_id), 0) + 1 AS identity_id`));
          })
          .then((res) => {
            identityId = parseInt(res[0].identity_id);
            // First insert the queried identifier with the identity_id
            return knex('IdentityAttributes').insert({
              identity_id: identityId,
              name: options.id[0],
              value: options.id[1],
              viewpoint_name: options.viewpoint[0],
              viewpoint_value: options.viewpoint[1],
              confirmations: 1,
              refutations: 0,
            });
          })
          .then(() => {
            let last;
            function generateSubQuery() {
              return knex
                .from('Messages as m')
                .innerJoin('MessageAttributes as attr1', (q) => {
                  q.on('m.hash', '=', 'attr1.message_hash');
                })
                .innerJoin('IdentityAttributes as ia', (q) => {
                  q.on('ia.name', '=', 'attr1.name');
                  q.on('ia.value', '=', 'attr1.value');
                  q.on('ia.identity_id', '=', identityId);
                })
                .innerJoin('MessageAttributes as attr2', (q) => {
                  q.on('m.hash', '=', 'attr2.message_hash');
                  q.on('attr2.is_recipient', '=', 'attr1.is_recipient');
                })
                .innerJoin('UniqueIdentifierTypes as uidt', 'uidt.name', 'attr1.name')
                .innerJoin('TrustDistances as td_signer', (q) => {
                  q.on('td_signer.start_attr_name', '=', knex.raw('?', options.viewpoint[0]));
                  q.on('td_signer.start_attr_value', '=', knex.raw('?', options.viewpoint[1]));
                  q.on('td_signer.end_attr_name', '=', knex.raw('?', 'keyID'));
                  q.on('td_signer.end_attr_value', '=', 'm.signer_keyid');
                });
            }

            function generateDeleteSubQuery() {
              return generateSubQuery()
                // Select for deletion the related identity attributes that were previously inserted
                // with a different identity_id
                .innerJoin('IdentityAttributes as existing', (q) => {
                  q.on('existing.identity_id', '!=', identityId);
                  q.on('existing.name', '=', 'attr2.name');
                  q.on('existing.value', '=', 'attr2.value');
                  q.on('existing.viewpoint_name', '=', knex.raw('?', options.viewpoint[0]));
                  q.on('existing.viewpoint_value', '=', knex.raw('?', options.viewpoint[1]));
                })
                .innerJoin('UniqueIdentifierTypes as uidt2', 'uidt2.name', 'existing.name')
                .select('existing.identity_id');
            }

            function generateInsertSubQuery() {
              return generateSubQuery()
                // Select for insertion the related identity attributes that do not already exist
                // on the identity_id
                .leftJoin('IdentityAttributes as existing', (q) => {
                  q.on('existing.identity_id', '=', identityId);
                  q.on('existing.name', '=', 'attr2.name');
                  q.on('existing.value', '=', 'attr2.value');
                  q.on('existing.viewpoint_name', '=', knex.raw('?', options.viewpoint[0]));
                  q.on('existing.viewpoint_value', '=', knex.raw('?', options.viewpoint[1]));
                })
                .whereNull('existing.identity_id')
                .select(
                  identityId,
                  'attr2.name',
                  'attr2.value',
                  knex.raw('?', options.viewpoint[0]),
                  knex.raw('?', options.viewpoint[1]),
                  knex.raw('SUM(CASE WHEN m.type = \'verify_identity\' THEN 1 ELSE 0 END)'),
                  knex.raw('SUM(CASE WHEN m.type = \'unverify_identity\' THEN 1 ELSE 0 END)'),
                ).groupBy('attr2.name', 'attr2.value');
            }

            function iterateSearch() {
              return knex('IdentityAttributes').whereIn('identity_id', generateDeleteSubQuery()).del()
                .then(() => knex('IdentityAttributes').insert(generateInsertSubQuery()))
                .then((res) => {
                  if (JSON.stringify(last) !== JSON.stringify(res)) {
                    last = res;
                    return iterateSearch();
                  }
                });
            }

            return iterateSearch();
          })
          .then(() => {
            const hasSearchedAttributes = options.searchedAttributes &&
              options.searchedAttributes.length > 0;

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
      })
        .then(res => pub.getStats(options.id, { viewpoint: options.viewpoint, maxDistance: 0 })
          .then(() => res));
    },

    getTrustDistance(from, to) {
      if (from[0] === to[0] && from[1] === to[1]) {
        return Promise.resolve(0);
      }
      return knex.select('distance').from('TrustDistances').where({
        start_attr_name: from[0],
        start_attr_value: from[1],
        end_attr_name: to[0],
        end_attr_value: to[1],
      }).then((res) => {
        let distance = -1;
        if (res.length) {
          distance = res[0].distance;
        }
        return Promise.resolve(distance);
      });
    },

    getTrustDistances(from) {
      return knex.select('*').from('TrustDistances').where({
        start_attr_name: from[0],
        start_attr_value: from[1],
      });
    },

    /*
      1. build a web of trust consisting of keyIDs only
      2. build a web of trust consisting of all kinds of unique attributes, sourcing from messages
          signed by keyIDs in our web of trust
    */
    generateWebOfTrustIndex(id, maxDepth, maintain, trustedKeyID) {
      if (id[0] !== 'keyID' && !trustedKeyID) {
        throw new Error('Please specify a trusted keyID');
      }
      const trustedKey = trustedKeyID ? ['keyID', trustedKeyID] : id;

      /*
        Can create TrustDistances based on messages authored by startId, or transitively, from messages
        authored by identities that have a TrustDistance from startId (when depth > 1)
      */
      function buildQuery(betweenKeyIDsOnly, trx, depth) {
        const startId = betweenKeyIDsOnly ? trustedKey : id;
        const subQuery = trx.distinct(
          trx.raw('?', startId[0]),
          trx.raw('?', startId[1]),
          'attr2.name',
          'attr2.value',
          depth,
        )
          .select()
          .from('Messages as m')
          .innerJoin('MessageAttributes as attr1', (q) => {
            q.on('m.hash', '=', 'attr1.message_hash');
            if (depth === 1) {
              q.on('attr1.name', '=', trx.raw('?', startId[0]));
              q.on('attr1.value', '=', trx.raw('?', startId[1]));
            }
            q.on('attr1.is_recipient', '=', trx.raw('?', false));
          })
          .innerJoin('MessageAttributes as attr2', (q) => {
            q.on('m.hash', '=', 'attr2.message_hash');
            if (betweenKeyIDsOnly) {
              q.on('attr2.name', '=', trx.raw('?', 'keyID'));
            }
            q.on('attr2.is_recipient', '=', trx.raw('?', true));
          });

        if (depth > 1) {
          subQuery.innerJoin('TrustDistances as td_author', (q) => {
            q.on('td_author.start_attr_name', '=', trx.raw('?', startId[0]));
            q.on('td_author.start_attr_value', '=', trx.raw('?', startId[1]));
            q.on('td_author.end_attr_name', '=', 'attr1.name');
            q.on('td_author.end_attr_value', '=', 'attr1.value');
            q.on('td_author.distance', '=', trx.raw('?', depth - 1));
          });
        }

        // Where not exists
        subQuery.leftJoin('TrustDistances as td_recipient', (q) => {
          q.on('td_recipient.start_attr_name', '=', trx.raw('?', startId[0]));
          q.on('td_recipient.start_attr_value', '=', trx.raw('?', startId[1]));
          q.on('td_recipient.end_attr_name', '=', 'attr2.name');
          q.on('td_recipient.end_attr_value', '=', 'attr2.value');
        });
        subQuery.whereNull('td_recipient.distance');

        if (!betweenKeyIDsOnly) {
          subQuery.innerJoin('UniqueIdentifierTypes as uidt1', 'uidt1.name', 'attr1.name');
          subQuery.innerJoin('UniqueIdentifierTypes as uidt2', 'uidt2.name', 'attr2.name');
          subQuery.leftJoin('TrustDistances as td_signer', (q) => {
            q.on('td_signer.start_attr_name', '=', trx.raw('?', trustedKey[0]));
            q.on('td_signer.start_attr_value', '=', trx.raw('?', trustedKey[1]));
            q.on('td_signer.end_attr_name', '=', trx.raw('?', 'keyID'));
            q.on('td_signer.end_attr_value', '=', 'm.signer_keyid');
          });
          subQuery.where((q) => {
            q.whereNotNull('td_signer.distance').orWhere('m.signer_keyid', trustedKey[1]);
          });
        }
        subQuery.where('m.is_latest', true)
          .andWhere('m.rating', '>', trx.raw('(m.min_rating + m.max_rating) / 2'));

        return trx('TrustDistances').insert(subQuery).return();
      }

      let q;
      let q2;
      if (maintain) {
        q = this.addTrustIndexedAttribute(id, maxDepth);
      } else {
        q = Promise.resolve();
      }
      q = q.then(() => pub.mapIdentityAttributes({ id, viewpoint: id }));
      let i;
      return q = q.then(() => knex.transaction(trx => trx('TrustDistances')
        .where({ start_attr_name: id[0], start_attr_value: id[1] }).del()
        .then(() => trx('TrustDistances').where({ start_attr_name: trustedKey[0], start_attr_value: trustedKey[1] }).del())
        .then(() =>
          // Add trust distance to self = 0
          trx('TrustDistances')
            .insert({
              start_attr_name: id[0], start_attr_value: id[1], end_attr_name: id[0], end_attr_value: id[1], distance: 0,
            })
            .then(() => {
              if (trustedKey[0] !== id[0] && trustedKey[1] !== id[1]) {
                return trx('TrustDistances')
                  .insert({
                    start_attr_name: trustedKey[0],
                    start_attr_value: trustedKey[1],
                    end_attr_name: trustedKey[0],
                    end_attr_value: trustedKey[1],
                    distance: 0,
                  })
                  .return();
              }
            }))
        .then(() => {
          q2 = Promise.resolve();
          for (i = 1; i <= maxDepth; i += 1) {
            q2.then(buildQuery(true, trx, i));
          }
          return q2;
        })
        .then(() => {
          q2 = Promise.resolve();
          for (i = 1; i <= maxDepth; i += 1) {
            q2.then(buildQuery(false, trx, i));
          }
          return q2;
        })
        .then(() => trx('TrustDistances')
          .where({ start_attr_name: id[0], start_attr_value: id[1] })
          .count('* as wot_size'))
        .then(res => parseInt(res[0].wot_size))));
    },

    generateIdentityIndex(viewpoint) { // possible param: trustedkeyid
      console.log('Generating identity index (SQL)');
      function mapNextIdentifier() {
        return knex('TrustDistances')
          .leftJoin('IdentityAttributes', (q) => {
            q.on('IdentityAttributes.viewpoint_name', '=', 'TrustDistances.start_attr_name');
            q.on('IdentityAttributes.viewpoint_value', '=', 'TrustDistances.start_attr_value');
            q.on('IdentityAttributes.name', '=', 'TrustDistances.end_attr_name');
            q.on('IdentityAttributes.value', '=', 'TrustDistances.end_attr_value');
          })
          .where({
            start_attr_name: viewpoint[0],
            start_attr_value: viewpoint[1],
            identity_id: null,
          })
          .orderBy('distance', 'asc')
          .limit(1)
          .select('end_attr_name', 'end_attr_value')
          .then((res) => {
            if (res.length) {
              const id = [res[0].end_attr_name, res[0].end_attr_value];
              process.stdout.write('*');
              return pub.mapIdentityAttributes({ id, viewpoint })
                .then(() => mapNextIdentifier());
            }
          });
      }

      // for each identifier in WoT: map identity, unless identifier already belongs to an identity
      return knex('IdentityAttributes').where({ viewpoint_name: viewpoint[0], viewpoint_value: viewpoint[1] }).del()
        .then(() => knex('IdentityStats').where({ viewpoint_name: viewpoint[0], viewpoint_value: viewpoint[1] }).del())
        .then(() => pub.mapIdentityAttributes({ id: viewpoint, viewpoint }))
        .then(() => mapNextIdentifier());
    },

    addTrustIndexedAttribute(id, depth) {
      return knex('TrustIndexedAttributes').where({ name: id[0], value: id[1] }).count('* as count')
        .then((res) => {
          if (parseInt(res[0].count)) {
            return knex('TrustIndexedAttributes').where({ name: id[0], value: id[1] }).update({ depth });
          }
          return knex('TrustIndexedAttributes').insert({ name: id[0], value: id[1], depth });
        })
        .then(() => pub.getTrustIndexedAttributes(true))
        .then((res) => {
          pub.trustIndexedAttributes = res;
          return knex('TrustDistances')
            .where({
              start_attr_name: id[0],
              start_attr_value: id[1],
              end_attr_name: id[0],
              end_attr_value: id[1],
            })
            .count('* as c');
        })
        .then((res) => {
          if (parseInt(res[0].c) === 0) {
          // Add trust distance to self = 0
            return knex('TrustDistances')
              .insert({
                start_attr_name: id[0],
                start_attr_value: id[1],
                end_attr_name: id[0],
                end_attr_value: id[1],
                distance: 0,
              }).return();
          }
        });
    },

    getTrustIndexedAttributes(forceRefresh) {
      if (pub.trustIndexedAttributes && !forceRefresh) {
        return Promise.resolve(pub.trustIndexedAttributes);
      }
      return knex('TrustIndexedAttributes').select('*').then((res) => {
        pub.trustIndexedAttributes = res;
        return res;
      });
    },

    getUniqueIdentifierTypes() {
      return knex('UniqueIdentifierTypes').select('name')
        .then((types) => {
          pub.uniqueIdentifierTypes = [];
          types.forEach((type) => {
            pub.uniqueIdentifierTypes.push(type.name);
          });
        });
    },

    isUniqueType(type) {
      return pub.uniqueIdentifierTypes.indexOf(type) > -1;
    },

    getTrustPaths() { // start, end, maxLength, shortestOnly, viewpoint, limit
      return Promise.resolve([]); // Disabled until the performance is improved
    },

    getMessageCount() {
      return knex('Messages').count('* as val')
        .then(res => parseInt(res[0].val));
    },

    getStats(id, options) {
      let sentSql = '';
      sentSql += 'SUM(CASE WHEN m.rating > (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS sent_positive, ';
      sentSql += 'SUM(CASE WHEN m.rating = (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS sent_neutral, ';
      sentSql += 'SUM(CASE WHEN m.rating < (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS sent_negative ';

      let dbTrue = true;
      let dbFalse = false;
      if (config.db.dialect === 'sqlite3') { // sqlite in test env fails otherwise, for some reason
        dbTrue = 1;
        dbFalse = 0;
      }

      let sent = knex('Messages as m')
        .innerJoin('MessageAttributes as author', (q) => {
          q.on('author.message_hash', '=', 'm.hash');
          q.on('author.is_recipient', '=', knex.raw('?', dbFalse));
        })
        .where('m.type', 'rating')
        .where('m.public', dbTrue);

      let receivedSql = '';
      receivedSql += 'SUM(CASE WHEN m.rating > (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS received_positive, ';
      receivedSql += 'SUM(CASE WHEN m.rating = (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS received_neutral, ';
      receivedSql += 'SUM(CASE WHEN m.rating < (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS received_negative, ';
      receivedSql += 'MIN(m.timestamp) AS first_seen ';

      let received = knex('Messages as m')
        .innerJoin('MessageAttributes as recipient', (q) => {
          q.on('recipient.message_hash', '=', 'm.hash');
          q.on('recipient.is_recipient', '=', knex.raw('?', dbTrue));
        })
        .where('m.type', 'rating')
        .where('m.public', dbTrue);

      let identityId;
      let identityIdQuery = Promise.resolve();
      if (options.viewpoint && options.maxDistance > -1) {
        identityIdQuery = knex('IdentityAttributes')
          .where({
            name: id[0],
            value: id[1],
            viewpoint_name: options.viewpoint[0],
            viewpoint_value: options.viewpoint[1],
          })
          .select('identity_id');

        identityIdQuery.then((res) => {
          if (res.length) {
            identityId = res[0].identity_id;

            sent.select('ia.identity_id as identity_id', 'm.hash');
            sent.innerJoin('IdentityAttributes as ia', (q) => {
              q.on('author.name', '=', 'ia.name');
              q.on('author.value', '=', 'ia.value');
            });
            sent.where('ia.identity_id', identityId);
            sent.where('m.priority', '>', 0);
            sent.groupBy('m.hash', 'ia.identity_id');

            const sentSubquery = knex.raw(sent).wrap('(', ') s');
            sent = knex('Messages as m')
              .select(knex.raw(sentSql))
              .innerJoin(sentSubquery, 'm.hash', 's.hash')
              .groupBy('s.identity_id');

            received.select('ia.identity_id as identity_id', 'm.hash as hash');
            received.innerJoin('IdentityAttributes as ia', (q) => {
              q.on('recipient.name', '=', 'ia.name');
              q.on('recipient.value', '=', 'ia.value');
            });
            received.where('ia.identity_id', identityId);
            received.groupBy('m.hash', 'ia.identity_id');

            received.innerJoin('TrustDistances as td', (q) => {
              q.on('td.start_attr_name', '=', knex.raw('?', options.viewpoint[0]));
              q.andOn('td.start_attr_value', '=', knex.raw('?', options.viewpoint[1]));
              q.andOn('td.end_attr_name', '=', 'ia.name');
              q.andOn('td.end_attr_value', '=', 'ia.value');
              if (options.maxDistance > 0) {
                q.andOn('td.distance', '<=', options.maxDistance);
              }
            });

            const receivedSubquery = knex.raw(received).wrap('(', ') s');
            received = knex('Messages as m')
              .select(knex.raw(receivedSql))
              .innerJoin(receivedSubquery, 'm.hash', 's.hash')
              .groupBy('s.identity_id');
          }
        });
      }
      return identityIdQuery.then(() => {
        if (!identityId) {
          sent.where({ 'author.name': id[0], 'author.value': id[1] });
          sent.groupBy('author.name', 'author.value');
          sent.select(knex.raw(sentSql));
          received.where({ 'recipient.name': id[0], 'recipient.value': id[1] });
          received.groupBy('recipient.name', 'recipient.value');
          received.select(knex.raw(receivedSql));
        }

        return Promise.all([sent, received]).then((response) => {
          const res = Object.assign({}, response[0][0], response[1][0]);
          for (const key in res) {
            if (key.indexOf('sent_') === 0 || key.indexOf('received_') === 0) {
              res[key] = parseInt(res[key]);
            }
          }

          if (options.viewpoint && !options.maxDistance) {
            const identityIds = [];
            if (identityId) {
              identityIds.push(identityId);
            }
            knex('IdentityStats')
              .where('identity_id', 'in', identityIds)
              .delete()
              .then(() => {
                if (identityId) {
                  knex('IdentityStats')
                    .insert({
                      identity_id: identityId,
                      viewpoint_name: options.viewpoint[0],
                      viewpoint_value: options.viewpoint[1],
                      positive_score: res.received_positive || 0,
                      negative_score: res.received_negative || 0,
                    }).return();
                }
              });
          }

          return Promise.resolve(res);
        });
      });
    },

    checkDefaultTrustList() {
      const _ = this;
      return knex('Messages').count('* as count')
        .then((res) => {
          if (parseInt(res[0].count) === 0) {
            const queries = [];
            const message = Message.createRating({
              author: [myId],
              recipient: [['keyID', '/pbxjXjwEsojbSfdM3wGWfE24F4fX3GasmoHXY3yYPM=']],
              comment: 'Identifi seed node, trusted by default',
              rating: 10,
              context: 'identifi_network',
              public: false,
            });
            const message2 = Message.createRating({
              author: [myId],
              recipient: [['nodeID', 'Qmbb1DRwd75rZk5TotTXJYzDSJL6BaNT1DAQ6VbKcKLhbs']],
              comment: 'Identifi IPFS seed node, trusted by default',
              rating: 10,
              context: 'identifi_network',
              public: false,
            });
            Message.sign(message, myKey.private.pem, myKey.public.hex);
            Message.sign(message2, myKey.private.pem, myKey.public.hex);
            queries.push(_.saveMessage(message));
            queries.push(_.saveMessage(message2));
            return Promise.all(queries);
          }
        });
    },

    ensureFreeSpace() {
      return this.getMessageCount()
        .then((res) => {
          if (res > config.maxMessageCount) {
            const nMessagesToDelete = Math.min(100, Math.ceil(config.maxMessageCount / 10));
            const messagesToDelete = knex('Messages')
              .select('hash')
              .limit(nMessagesToDelete)
              .orderBy('priority', 'asc')
              .orderBy('created', 'asc');
            return knex('Messages')
              .whereIn('hash', messagesToDelete).del()
              .then(() => {
                knex('MessageAttributes')
                  .whereIn('message_hash', messagesToDelete)
                  .del();
              });
          }
        });
    },
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
    getPriority(message) {
      const maxPriority = 100;
      let priority;

      message.signerKeyHash = Message.getSignerKeyHash(message);

      return pub.getTrustDistance(myId, ['keyID', message.signerKeyHash])
        .then((distanceToSigner) => {
          if (distanceToSigner === -1) { // Unknown signer
            return Promise.resolve(0);
          }
          let i;
          const queries = [];
          // Get distances to message authors
          for (i = 0; i < message.signedData.author.length; i += 1) {
            const q = pub.getTrustDistance(myId, message.signedData.author[i]);
            queries.push(q);
          }

          return Promise.all(queries).then((authorDistances) => {
            let shortestDistanceToAuthor = 10000000;
            for (let j = 0; j < authorDistances.length; j += 1) {
              if (authorDistances[j] > -1 && authorDistances[j] < shortestDistanceToAuthor) {
                shortestDistanceToAuthor = authorDistances[j];
              }
            }

            priority = maxPriority / (distanceToSigner + 1);
            priority = (priority / 2) + (priority / (shortestDistanceToAuthor + 2));
            priority = Math.round(priority);

            let hasAuthorKeyID;
            let hasRecipientKeyID;
            for (i = 0; i < message.signedData.author.length; i += 1) {
              if (message.signedData.author[i][0] === 'keyID') {
                hasAuthorKeyID = true;
                break;
              }
            }
            for (i = 0; i < message.signedData.recipient.length; i += 1) {
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

    isLatest() { // param: message
      return true; // TODO: implement
    },

    saveTrustDistance(startId, endId, distance) {
      return knex('TrustDistances').where({
        start_attr_name: startId[0],
        start_attr_value: startId[1],
        end_attr_name: endId[0],
        end_attr_value: endId[1],
      }).del()
        .then(() => knex('TrustDistances').insert({
          start_attr_name: startId[0],
          start_attr_value: startId[1],
          end_attr_name: endId[0],
          end_attr_value: endId[1],
          distance,
        }));
    },

    saveIdentityAttribute(identifier, viewpoint, identityId, confirmations, refutations) {
      return knex('IdentityAttributes')
        .insert({
          viewpoint_name: viewpoint[0],
          viewpoint_value: viewpoint[1],
          name: identifier[0],
          value: identifier[1],
          identity_id: identityId,
          confirmations,
          refutations,
        });
    },

    getIdentityID(identifier, viewpoint) {
      return knex
        .from('IdentityAttributes')
        .where({
          viewpoint_name: viewpoint[0],
          viewpoint_value: viewpoint[1],
          name: identifier[0],
          value: identifier[1],
        })
        .innerJoin('UniqueIdentifierTypes', 'IdentityAttributes.name', 'UniqueIdentifierTypes.name')
        .select('IdentityAttributes.identity_id');
    },

    updateIdentityIndexesByMessage(message) { // param: trustedKeyID
      if (message.signedData.type !== 'verify_identity' && message.signedData.type !== 'unverify_identity') {
        return Promise.resolve();
      }

      const queries = [];

      // TODO: make this faster
      return pub.getTrustIndexedAttributes().then((viewpoints) => {
        for (let j = 0; j < viewpoints.length; j += 1) {
          const viewpoint = [viewpoints[j].name, viewpoints[j].value];
          for (let i = 0; i < message.signedData.recipient.length; i += 1) {
            const m = pub.mapIdentityAttributes({ id: message.signedData.recipient[i], viewpoint });
            queries.push(m);
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

    deletePreviousMessage(message, deleteFromIpfsIndexes) {
      let i;
      let j;
      const verifyTypes = ['verify_identity', 'unverify_identity'];
      const t = message.signedData.type;
      const isVerifyMsg = verifyTypes.indexOf(t) > -1;

      function getHashesQuery(author, recipient) {
        const q = knex('Messages as m')
          .distinct('m.hash as hash', 'm.ipfs_hash as ipfs_hash', 'm.timestamp as timestamp', 'td.distance as distance')
          .innerJoin('MessageAttributes as author', (qq) => {
            qq.on('author.message_hash', '=', 'm.hash');
            qq.andOn('author.is_recipient', '=', knex.raw('?', false));
          })
          .innerJoin('MessageAttributes as recipient', (qq) => {
            qq.on('recipient.message_hash', '=', 'm.hash');
            qq.andOn('recipient.is_recipient', '=', knex.raw('?', true));
          })
          .innerJoin('UniqueIdentifierTypes as ia1', 'ia1.name', 'author.name')
          .leftJoin('TrustDistances as td', (qq) => {
            qq.on('td.start_attr_name', '=', knex.raw('?', myId[0]));
            qq.andOn('td.start_attr_value', '=', knex.raw('?', myId[1]));
            qq.andOn('td.end_attr_name', '=', 'author.name');
            qq.andOn('td.end_attr_value', '=', 'author.value');
          })
          .where({
            'm.signer_keyid': message.signerKeyHash,
            'author.name': author[0],
            'author.value': author[1],
          })
          .orderBy('m.timestamp', 'DESC');

        if (recipient) {
          q.where({
            'recipient.name': recipient[0],
            'recipient.value': recipient[1],
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

      let hashes = [];
      let ipfsIndexKeys = [];

      function addHashes(res) {
        for (i = 0; i < res.length; i += 1) {
          hashes.push(res[i].hash);
          if (deleteFromIpfsIndexes && res[i].ipfs_hash && res[i].ipfs_hash.length) {
            const msg = res[i];
            ipfsIndexKeys.push(pub.getMsgIndexKey(msg));
          }
        }
      }

      function getAndAddHashes(author, recipient) {
        return getHashesQuery(author, recipient)
          .then(addHashes);
      }

      function addInnerJoinMessageRecipient(query, recipient, n) {
        const as = `recipient${n}`;
        query.innerJoin(`MessageAttributes as ${as}`, (q) => {
          q.on(`${as}.message_hash`, '=', 'm.hash');
          q.on(`${as}.is_recipient`, '=', knex.raw('?', true));
          q.on(`${as}.name`, '=', knex.raw('?', recipient[0]));
          q.on(`${as}.value`, '=', knex.raw('?', recipient[1]));
        });
      }

      let queries = [];

      // Delete previous verify or unverify with the exact same recipient attributes
      if (isVerifyMsg) {
        for (i = 0; i < message.signedData.author.length; i += 1) {
          let q = getHashesQuery(message.signedData.author[i]);
          for (j = 0; j < message.signedData.recipient.length; j += 1) {
            addInnerJoinMessageRecipient(q, message.signedData.recipient[j], j);
          }
          queries.push(q = q.then(addHashes));
        }
      }

      // Delete possible previous msg from A->B (created less than minMessageInterval ago?)
      for (i = 0; i < message.signedData.author.length; i += 1) {
        for (j = 0; j < message.signedData.recipient.length; j += 1) {
          const h = getAndAddHashes(message.signedData.author[i], message.signedData.recipient[j]);
          queries.push(h);
        }
      }

      return Promise.all(queries).then(() => {
        queries = [];
        hashes = util.removeDuplicates(hashes);
        for (i = 0; i < hashes.length; i += 1) {
          queries.push(pub.dropMessage(hashes[i]));
        }
        ipfsIndexKeys = util.removeDuplicates(ipfsIndexKeys);
        for (i = 0; i < ipfsIndexKeys.length; i += 1) {
          console.log('deleting from index', ipfsIndexKeys[i], ipfsIndexKeys[i].substr(ipfsIndexKeys[i].indexOf(':') + 1));
          queries.push(p.ipfsMessagesByDistance.delete(ipfsIndexKeys[i]));
          queries.push(p.ipfsMessagesByTimestamp.delete(ipfsIndexKeys[i].substr(ipfsIndexKeys[i].indexOf(':') + 1)));
        }
        return Promise.all(queries);
      });
    },

    updateWotIndexesByMessage(message) {
      const queries = [];

      // TODO: remove trust distance if a previous positive rating is replaced

      function makeSubquery(author, recipient) {
        return knex
          .from('TrustIndexedAttributes AS viewpoint')
          .innerJoin('UniqueIdentifierTypes as ia', 'ia.name', knex.raw('?', recipient[0]))
          .innerJoin('TrustDistances AS td', (q) => {
            q.on('td.start_attr_name', '=', 'viewpoint.name')
              .andOn('td.start_attr_value', '=', 'viewpoint.value')
              .andOn('td.end_attr_name', '=', knex.raw('?', author[0]))
              .andOn('td.end_attr_value', '=', knex.raw('?', author[1]));
          })
          .leftJoin('TrustDistances AS existing', (q) => { // TODO: update existing if new distance is shorter
            q.on('existing.start_attr_name', '=', 'viewpoint.name')
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
            knex.raw(`${SQL_IFNULL}(td.distance, 0) + 1 as distance`),
          );
      }

      function getSaveFunction(author, recipient) {
        return (distance) => {
          if (distance > -1) {
            return knex('TrustDistances').insert(makeSubquery(author, recipient));
          }
        };
      }

      if (Message.isPositive(message)) {
        let i;
        let j;
        for (i = 0; i < message.signedData.author.length; i += 1) {
          const author = message.signedData.author[i];
          const t = author[0] === 'keyID' ? author[1] : myKey.hash; // trusted key
          for (j = 0; j < message.signedData.recipient.length; j += 1) {
            const recipient = message.signedData.recipient[j];
            const q = pub.getTrustDistance(['keyID', t], ['keyID', message.signerKeyHash])
              .then(getSaveFunction(author, recipient));
            queries.push(q);
          }
        }
      }

      return Promise.all(queries);
    },

    getIndexesFromIpfsRoot() {
      return knex('TrustIndexedAttributes')
        .where({ name: myId[0], value: myId[1] })
        .whereNotNull('ipfs_index_root')
        .select('ipfs_index_root')
        .then((res) => {
          let q = Promise.resolve();
          if (res.length) {
            q = q.then(() => p.ipfs.object.links(res[0].ipfs_index_root)
              .then((links) => {
                const queries = [];
                links.forEach((link) => {
                  switch (link._name) {
                    case 'messages_by_distance':
                      queries.push(btree.MerkleBTree.getByHash(link._multihash, p.ipfsStorage)
                        .then((index) => {
                          p.ipfsMessagesByDistance = index;
                        }));
                      break;
                    case 'messages_by_timestamp':
                      queries.push(btree.MerkleBTree.getByHash(link._multihash, p.ipfsStorage)
                        .then((index) => {
                          p.ipfsMessagesByTimestamp = index;
                        }));
                      break;
                    case 'identities_by_distance':
                      queries.push(btree.MerkleBTree.getByHash(link._multihash, p.ipfsStorage)
                        .then((index) => {
                          p.ipfsIdentitiesByDistance = index;
                        }));
                      break;
                    case 'identities_by_searchkey':
                      queries.push(btree.MerkleBTree.getByHash(link._multihash, p.ipfsStorage)
                        .then((index) => {
                          p.ipfsIdentitiesBySearchKey = index;
                        }));
                      break;
                    default:
                      break;
                  }
                });
                return Promise.all(queries);
              }));
          }
          q = timeoutPromise(q, 15000);
          return q.then(() => {
            p.ipfsIdentitiesBySearchKey = p.ipfsIdentitiesBySearchKey ||
              new btree.MerkleBTree(p.ipfsStorage, 100);
            p.ipfsIdentitiesByDistance = p.ipfsIdentitiesByDistance ||
              new btree.MerkleBTree(p.ipfsStorage, 100);
            p.ipfsMessagesByDistance = p.ipfsMessagesByDistance ||
              new btree.MerkleBTree(p.ipfsStorage, 100);
            p.ipfsMessagesByTimestamp = p.ipfsMessagesByTimestamp ||
              new btree.MerkleBTree(p.ipfsStorage, 100);
          });
        });
    },
  };

  pub.init = (conf, ipfs) => {
    if (ipfs) {
      p.ipfs = ipfs;
      p.ipfsStorage = new btree.IPFSStorage(p.ipfs);
    }
    config = conf;
    if (conf.db.client === 'pg') {
      SQL_IFNULL = 'COALESCE';
    }
    return schema.init(knex, config)
      .then(() => pub.getUniqueIdentifierTypes())
      .then(() => pub.addTrustIndexedAttribute(myId, myTrustIndexDepth))
    // TODO: if myId is changed, the old one should be removed from TrustIndexedAttributes
      .then(() => pub.mapIdentityAttributes({ id: myId }))
      .then(() => pub.checkDefaultTrustList())
      .then(() => {
        if (p.ipfsStorage) {
          return p.getIndexesFromIpfsRoot();
        }
      })
      .then(() => {
        if (!p.ipfsStorage) {
          return;
        }
        if (process.env.NODE_ENV !== 'test') {
          pub.saveMessagesFromIpfsIndexes(); // non-blocking
        }
        pub.keepAddingNewMessagesToIpfsIndex();
      });
  };

  return pub;
};
