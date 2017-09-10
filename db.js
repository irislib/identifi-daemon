const util = require('./util.js');
const schema = require('./schema.js');

const crypto = require('crypto');
const btree = require('merkle-btree');

const Message = require('identifi-lib/message');
const keyutil = require('identifi-lib/keyutil');

const dagPB = require('ipld-dag-pb');

const MY_KEY = keyutil.getDefault();
const MY_ID = ['keyID', MY_KEY.hash];
const MY_TRUST_INDEX_DEPTH = 4;
const IPFS_INDEX_WIDTH = 200;
const REBUILD_INDEXES_IF_NEW_MSGS_GT = 30;

let config;
let SQL_IFNULL = 'IFNULL';

module.exports = (knex) => {
  let p; // Private methods
  let lastIpfsIndexedMessageSavedAt = (new Date()).toISOString();
  let ipfsIdentityIndexKeysToRemove = {};

  const pub = {
    trustIndexedAttributes: null,
    async saveMessage(msg, updateTrustIndexes = true, addToIpfs = true) {
      const message = msg;
      if (typeof message.signerKeyHash === 'undefined') {
        message.signerKeyHash = Message.getSignerKeyHash(message);
      }
      await this.ensureFreeSpace();
      // Unobtrusively store msg to ipfs
      if ((p.ipfs && !message.ipfs_hash) && addToIpfs) {
        const res = await pub.addMessageToIpfs(message);
        message.ipfs_hash = res[0].hash;
      }
      const exists = await pub.messageExists(message.hash);

      if (exists) {
        if (addToIpfs && message.ipfs_hash) {
        // Msg was added to IPFS - update ipfs_hash
          return knex('Messages').where({ hash: message.hash }).update({ ipfs_hash: message.ipfs_hash });
        }
        return false;
      }
      if (Object.keys(ipfsIdentityIndexKeysToRemove).length < REBUILD_INDEXES_IF_NEW_MSGS_GT) {
        // Mark for deletion the index references to expiring identity profiles
        ipfsIdentityIndexKeysToRemove[message.hash] = [];
        const authorAttrs = await pub.getIdentityAttributesByAuthorOrRecipient(message, true);
        const authorKeys = await pub.getIndexKeysByIdentity(authorAttrs);
        ipfsIdentityIndexKeysToRemove[message.hash] =
          ipfsIdentityIndexKeysToRemove[message.hash].concat(authorKeys);
        const recipientAttrs = pub.getIdentityAttributesByAuthorOrRecipient(message, false);
        const recipientKeys = await pub.getIndexKeysByIdentity(recipientAttrs);
        ipfsIdentityIndexKeysToRemove[message.hash] =
          ipfsIdentityIndexKeysToRemove[message.hash].concat(recipientKeys);
      }
      const isPublic = typeof message.signedData.public === 'undefined' ? true : message.signedData.public;
      await p.deletePreviousMessage(message);
      const priority = await p.getPriority(message);
      await knex.transaction(async (trx) => {
        await trx('Messages').insert({
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
        });
        let i;
        const queries = [];
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
      });
      if (updateTrustIndexes) {
        await p.updateWotIndexesByMessage(message);
        await p.updateIdentityIndexesByMessage(message);
      }
      return message;
    },

    async keepAddingNewMessagesToIpfsIndex() {
      await pub.addNewMessagesToIpfsIndex();
      return new Promise(((resolve) => {
        setTimeout(() => {
          resolve(pub.keepAddingNewMessagesToIpfsIndex());
        }, 10000);
      }));
    },

    async addIndexesToIpfs() {
      await pub.addMessageIndexToIpfs();
      return pub.addIdentityIndexToIpfs();
    },

    async addNewMessagesToIpfsIndex() {
      try {
        const messages = await pub.getMessages({
          limit: 10000,
          orderBy: 'timestamp',
          direction: 'desc',
          viewpoint: MY_ID,
          savedAtGt: lastIpfsIndexedMessageSavedAt,
        });
        if (messages.length) {
          console.log('', messages.length, 'new messages to index');
        }
        /* rebuilding the indexes is more efficient than
        inserting large number of entries individually */
        if (messages.length < REBUILD_INDEXES_IF_NEW_MSGS_GT) {
          let q = Promise.resolve();
          // remove identity index entries that point to expired identity profiles
          Object.keys(ipfsIdentityIndexKeysToRemove).forEach((msg) => {
            ipfsIdentityIndexKeysToRemove[msg].forEach((key) => {
              q.then(() => {
                const q2 = p.ipfsIdentitiesByDistance.delete(key);
                const q3 = p.ipfsIdentitiesBySearchKey.delete(key.substr(key.indexOf(':') + 1));
                return util.timeoutPromise(Promise.all([q2, q3]), 30000);
              });
              delete ipfsIdentityIndexKeysToRemove[msg];
            });
          });
          messages.forEach((msg) => {
            const message = Message.decode(msg);
            const d = new Date(message.saved_at).toISOString();
            if (d > lastIpfsIndexedMessageSavedAt) {
              lastIpfsIndexedMessageSavedAt = d;
            }
            q = q.then(() => pub.addMessageToIpfsIndex(message));
          });
          const r = await util.timeoutPromise(q.return(messages.length), 200000);
          return typeof r === 'undefined' ? pub.addIndexesToIpfs() : r;
        }
        await pub.addIndexesToIpfs();
        ipfsIdentityIndexKeysToRemove = {};
        if (messages.length) {
          console.log('adding index root to ipfs');
          return pub.addIndexRootToIpfs();
        }
      } catch (e) {
        console.log('adding new messages to ipfs failed:', e);
      }
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

    async addMessageToIpfsIndex(msg) {
      try { // TODO: move these try/catches to calling function
        const message = msg;
        const msgIndexKey = pub.getMsgIndexKey(message); // TODO: should have distance
        await p.ipfsMessagesByDistance.put(msgIndexKey, message);
        await p.ipfsMessagesByTimestamp.put(msgIndexKey.substr(msgIndexKey.indexOf(':') + 1), message);
        const authorAttrs = await pub.getIdentityAttributesByAuthorOrRecipient(message, true);
        await pub.addIdentityToIpfsIndex(authorAttrs);

        const recipientAttrs = await pub.getIdentityAttributesByAuthorOrRecipient(message, false);
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
      } catch (e) {
        console.log('adding to ipfs failed:', e);
      }
    },

    addMessageToIpfs(message) {
      return p.ipfs.files.add(Buffer.from(message.jws, 'utf8'));
    },

    async addDbMessagesToIpfs() {
      let counter = 0;
      async function getReindexQuery() {
        const msgs = {};
        let r = await knex('Messages').whereNull('ipfs_hash').select('jws', 'hash').limit(100);
        if (r.length && p.ipfs) {
          r.forEach((msg) => {
            msgs[msg.hash] = Buffer.from(msg.jws, 'utf8');
          });
          try {
            r = await p.ipfs.files.add(Object.values(msgs));
          } catch (e) {
            console.log('adding to ipfs failed:', e);
          }
        } else {
          r = [];
        }
        console.log('added', r.length, 'msgs to ipfs');
        const queries = [];
        Object.keys(msgs).forEach((hash, index) => {
          queries.push(knex('Messages').where({ hash }).update({ ipfs_hash: r[index].hash }));
        });
        r = await Promise.all(queries);
        console.log('updated', r.length, 'db entries');
        if (r.length) {
          counter += 1;
          return getReindexQuery();
        }
      }

      await getReindexQuery();
      return `Reindexed ${counter} messages`;
    },

    async addIndexRootToIpfs() {
      // saves indexes as an IPFS directory
      // TODO: if info already exists, don't rewrite
      try {
        let indexRoot;
        const a = await pub.getIdentityAttributes({ id: MY_ID });
        let attrs = a;
        if (attrs.length) {
          attrs = attrs[0];
        } else {
          attrs = [];
        }
        let res = await p.ipfs.files.add([
          { path: 'info', content: Buffer.from(JSON.stringify({ keyID: MY_ID[1], attrs })) },
          { path: 'messages_by_distance', content: Buffer.from(p.ipfsMessagesByDistance.rootNode.serialize()) },
          { path: 'messages_by_timestamp', content: Buffer.from(p.ipfsMessagesByTimestamp.rootNode.serialize()) },
          { path: 'identities_by_searchkey', content: Buffer.from(p.ipfsIdentitiesBySearchKey.rootNode.serialize()) },
          { path: 'identities_by_distance', content: Buffer.from(p.ipfsIdentitiesByDistance.rootNode.serialize()) },
        ]);
        const links = [];
        for (let i = 0; i < res.length; i += 1) {
          links.push({ Name: res[i].path, Hash: res[i].hash, Size: res[i].size });
        }
        res = await new Promise(((resolve, reject) => {
          dagPB.DAGNode.create(Buffer.from('\u0008\u0001'), links, (err, dag) => {
            if (err) {
              reject(err);
            }
            resolve(p.ipfs.object.put(dag));
          });
        }));
        if (res._json.multihash) {
          indexRoot = res._json.multihash;
          return knex('TrustIndexedAttributes')
            .where({ name: MY_ID[0], value: MY_ID[1] })
            .update('ipfs_index_root', res._json.multihash)
            .return(res);
        }
        res = await Promise.resolve(res);
        if (p.ipfs.name && res._json.multihash) {
          console.log('publishing index', res._json.multihash);
          const r = await p.ipfs.name.publish(res._json.multihash, {});
          console.log('published index', r);
        }
        return indexRoot;
      } catch (e) {
        console.log('error publishing index', e);
      }
    },

    getIdentityProfileIndexKeys(identityProfile, hash) {
      const indexKeys = [];
      const attrs = identityProfile.attrs;
      for (let j = 0; j < attrs.length; j += 1) {
        let distance = parseInt(attrs[j].dist);
        distance = Number.isNaN(distance) ? 99 : distance;
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
          for (let l = 0; l < words.length; l += 1) {
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

    async getIdentityProfile(attrs) {
      const identityProfile = { attrs };
      if (!attrs.length) {
        return identityProfile;
      }
      let uniqueAttr = attrs[0];
      for (let i = 0; i < attrs.length; i += 1) {
        if (pub.isUniqueType(attrs[i].name)) {
          uniqueAttr = attrs[i];
        }
      }

      const received = await pub.getMessages({
        recipient: [uniqueAttr.name, uniqueAttr.val],
        limit: 10000,
        orderBy: 'timestamp',
        direction: 'asc',
        viewpoint: MY_ID,
      });
      let msgs = [];
      received.forEach((msg) => {
        msgs.push({
          key: `${Date.parse(msg.timestamp)}:${(msg.ipfs_hash || msg.hash).substr(0, 9)}`,
          value: msg, // TODO: save only ipfs_hash
          targetHash: null,
        });
      });
      try {
        if (msgs.length && p.ipfsStorage) {
          const receivedIndex = await btree.MerkleBTree
            .fromSortedList(msgs, IPFS_INDEX_WIDTH, p.ipfsStorage);
          identityProfile.received = receivedIndex.rootNode.hash;
        }
        const sent = await pub.getMessages({
          author: [uniqueAttr.name, uniqueAttr.val],
          limit: 10000,
          orderBy: 'timestamp',
          direction: 'asc',
          viewpoint: MY_ID,
        });
        msgs = [];
        sent.forEach((msg) => {
          msgs.push({
            key: `${Date.parse(msg.timestamp)}:${(msg.ipfs_hash || msg.hash).substr(0, 9)}`,
            value: { jws: msg.jws },
            targetHash: null,
          });
        });
        if (msgs.length && p.ipfsStorage) {
          const sentIndex = await btree.MerkleBTree
            .fromSortedList(msgs, IPFS_INDEX_WIDTH, p.ipfsStorage);
          identityProfile.sent = sentIndex.rootNode.hash;
        }
      } catch (e) {
        console.log('p.ipfs', p.ipfs);
        console.log('adding', attrs, 'failed:', e);
      }
      return identityProfile;
    },

    async getIndexKeysByIdentity(attrs) {
      const identityProfile = await pub.getIdentityProfile(attrs);
      const keys = [];
      const hash = crypto.createHash('md5').update(JSON.stringify(identityProfile)).digest('base64');
      pub.getIdentityProfileIndexKeys(identityProfile, hash).forEach((key) => {
        keys.push(key);
        keys.push(key.substr(key.indexOf(':') + 1));
      });
      return keys;
    },

    async addIdentityToIpfsIndex(attrs) {
      const ip = await pub.getIdentityProfile(attrs);
      console.log('adding identityprofile to ipfs', ip);
      const r = await p.ipfs.files.add(Buffer.from(JSON.stringify(ip), 'utf8'));
      if (r.length) {
        const hash = crypto.createHash('md5').update(JSON.stringify(ip)).digest('base64');
        let q = Promise.resolve();
        let q2 = Promise.resolve();
        pub.getIdentityProfileIndexKeys(ip, hash).forEach((key) => {
          console.log('adding key', key);
          console.log('and', key.substr(key.indexOf(':') + 1));
          q = q.then(p.ipfsIdentitiesByDistance.put(key, r[0].hash));
          q2 = q2.then(p.ipfsIdentitiesBySearchKey.put(key.substr(key.indexOf(':') + 1), r[0].hash));
        });
        return util.timeoutPromise(Promise.all([q, q2]), 30000);
      }
    },

    async addIdentityIndexToIpfs() {
      const maxIndexSize = 100000;
      const identityIndexEntriesToAdd = [];
      const identityProfilesByHash = {};
      const r = await this.getIdentityAttributes({ limit: maxIndexSize });
      console.log('Adding identity index of', r.length, 'entries to ipfs');

      async function iterate(i) {
        console.log(`${i}/${r.length}`);
        if (i >= r.length) {
          return;
        }
        const identityProfile = await pub.getIdentityProfile(r[i]);
        const hash = crypto.createHash('md5').update(JSON.stringify(identityProfile)).digest('base64');
        identityProfilesByHash[hash] = identityProfile;
        pub.getIdentityProfileIndexKeys(identityProfile, hash).forEach((key) => {
          identityIndexEntriesToAdd.push({ key, value: hash, targetHash: null });
        });
        return iterate(i + 1);
      }

      await iterate(0);
      const orderedKeys = Object.keys(identityProfilesByHash);

      async function addIdentityProfilesToIpfs() {
        if (!orderedKeys.length) {
          return;
        }
        const keys = orderedKeys.splice(0, 100);
        const values = [];
        keys.forEach((key) => {
          values.push(Buffer.from(JSON.stringify(identityProfilesByHash[key]), 'utf8'));
        });
        const rr = await p.ipfs.files.add(values);
        keys.forEach((key, i) => {
          if (i < rr.length && rr[i].hash) {
            identityProfilesByHash[key] = rr[i].hash;
          }
        });
        return addIdentityProfilesToIpfs();
      }

      await addIdentityProfilesToIpfs();
      identityIndexEntriesToAdd.forEach((entry) => {
        // eslint-disable-next-line no-param-reassign
        entry.value = identityProfilesByHash[entry.value];
      });

      console.log('building index identities_by_distance');
      p.ipfsIdentitiesByDistance = await btree.MerkleBTree.fromSortedList(
        identityIndexEntriesToAdd.sort(util.sortByKey).slice(),
        IPFS_INDEX_WIDTH,
        p.ipfsStorage,
      );
      identityIndexEntriesToAdd.forEach((entry) => {
        entry.key = entry.key.substr(entry.key.indexOf(':') + 1); // eslint-disable-line no-param-reassign
      });
      console.log('building index identities_by_searchkey');
      p.ipfsIdentitiesBySearchKey = await btree.MerkleBTree.fromSortedList(
        identityIndexEntriesToAdd.sort(util.sortByKey),
        IPFS_INDEX_WIDTH,
        p.ipfsStorage,
      );
      return pub.addIndexRootToIpfs();
    },

    getMsgIndexKey(msg) {
      let distance = parseInt(msg.distance);
      distance = Number.isNaN(distance) ? 99 : distance;
      distance = (`00${distance}`).substring(distance.toString().length); // pad with zeros
      const key = `${distance}:${Math.floor(Date.parse(msg.timestamp || msg.signedData.timestamp) / 1000)}:${(msg.ipfs_hash || msg.hash).substr(0, 9)}`;
      return key;
    },

    async addMessageIndexToIpfs() {
      const maxMsgCount = 100000;
      let msgsToIndex = [];
      async function iterate(limit = 10000, initialOffset = 0, initialDistance = 0) {
        let distance = initialDistance;
        let offset = initialOffset;
        const msgs = await pub.getMessages({
          limit,
          offset,
          orderBy: 'timestamp',
          direction: 'asc',
          viewpoint: MY_ID,
          where: { 'td.distance': distance },
        });
        if (msgs.length === 0 && offset === 0) {
          return msgs.length;
        }
        if (msgs.length < limit) {
          distance += 1;
          offset = 0;
        } else {
          offset += limit;
        }
        msgs.forEach((m) => {
          process.stdout.write('.');
          const msg = Message.decode(m);
          msg.distance = distance;
          const key = pub.getMsgIndexKey(msg);
          msgsToIndex.push({ key, value: msg, targetHash: null });
        });
        const hasMore = !(msgs.length === 0 && offset === 0);
        if (msgsToIndex.length < maxMsgCount && hasMore) {
          return iterate(limit, offset, distance);
        }
        return msgsToIndex.length;
      }

      console.log('adding msgs to ipfs');
      const r = await iterate();
      console.log('res', r);
      console.log('adding messages_by_distance index to ipfs');
      p.ipfsMessagesByDistance = await btree.MerkleBTree.fromSortedList(
        msgsToIndex.slice(),
        IPFS_INDEX_WIDTH,
        p.ipfsStorage,
      );

      // create index of messages sorted by timestamp
      msgsToIndex.forEach((msg) => {
        msg.key = msg.key.substr(msg.key.indexOf(':') + 1); // eslint-disable-line no-param-reassign
      });
      msgsToIndex = msgsToIndex.sort(util.sortByKey);
      console.log('adding messages_by_timestamp index to ipfs');
      p.ipfsMessagesByTimestamp = await btree.MerkleBTree.fromSortedList(
        msgsToIndex,
        IPFS_INDEX_WIDTH,
        p.ipfsStorage,
      );
      // Add message index to IPFS
      return pub.addIndexRootToIpfs();
    },

    async saveMessageFromIpfs(path) {
      const r = await knex('Messages').where('ipfs_hash', path).count('* as count');
      if (parseInt(r[0].count) === 0) {
        try {
          const buffer = await util.timeoutPromise(p.ipfs.files.cat(path, { buffer: true }), 5000);
          if (buffer) {
            const msg = { jws: buffer.toString('utf8'), ipfs_hash: path };
            process.stdout.write('+');
            Message.verify(msg);
            console.log('saving new msg from ipfs:', msg.ipfs_hash);
            return pub.saveMessage(msg);
          }
          process.stdout.write('-');
        } catch (e) {
          console.log('Processing message', path, 'failed:', e);
        }
      }
    },

    async saveMessagesFromIpfsIndex(ipnsName) {
      try {
        console.log('Getting path for name', ipnsName);
        if (!(p.ipfs && p.ipfs.name)) {
          console.log('ipfs.name is not available');
          return;
        }
        const r = await util.timeoutPromise(p.ipfs.name.resolve(ipnsName), 60000);
        if (!r) { throw new Error('Ipfs index name was not resolved', ipnsName); }
        let path = r.Path.replace('/ipfs/', '');
        console.log('resolved name', path);
        const links = await util.timeoutPromise(p.ipfs.object.links(path), 30000);
        if (!links) { throw new Error('Ipfs index was not resolved', ipnsName); }
        for (let i = 0; i < links.length; i += 1) {
          if (links[i]._name === 'messages_by_distance') {
            path = links[i]._multihash;
            break;
          }
        }
        if (!path) {
          throw new Error('No messages index found at', ipnsName);
        }
        console.log('Looking up index');
        const index = await btree.MerkleBTree.getByHash(path, p.ipfsStorage, IPFS_INDEX_WIDTH);
        const msgs = await index.searchText('', 100000);
        let q = Promise.resolve();
        console.log('Processing', msgs.length, 'messages from index');
        msgs.forEach((entry) => {
          const msg = { jws: entry.value.jws };
          if (Message.decode(msg)) {
            process.stdout.write('.');
            q = q.then(() => pub.saveMessage(msg, false, false));
          }
        });
        await q;
        console.log('Finished saving messages from index', ipnsName);
        return pub.addDbMessagesToIpfs();
      } catch (e) {
        console.log('Processing index', ipnsName, 'failed:', e);
      }
    },

    async saveMessagesFromIpfsIndexes() {
      const r = await knex('Messages')
        .innerJoin('MessageAttributes as author', (q) => {
          q.on('author.message_hash', '=', 'Messages.hash');
          q.andOn('author.is_recipient', '=', knex.raw('?', false));
        })
        .innerJoin('MessageAttributes as recipient', (q) => {
          q.on('recipient.message_hash', '=', 'Messages.hash');
          q.andOn('recipient.is_recipient', '=', knex.raw('?', true));
        })
        .innerJoin('TrustDistances as td', (q) => {
          q.on('td.start_attr_name', '=', knex.raw('?', MY_ID[0]));
          q.on('td.start_attr_value', '=', knex.raw('?', MY_ID[1]));
          q.on('td.end_attr_name', '=', 'author.name');
          q.on('td.end_attr_value', '=', 'author.value');
        })
        .where('td.distance', '<=', 2)
        .andWhere('recipient.name', 'nodeID')
        .select()
        .distinct('recipient.value');
      let i;
      let q = Promise.resolve();
      for (i = 0; i < r.length; i += 1) {
        try {
          q = q.then(pub.saveMessagesFromIpfsIndex(r[i].value));
        } catch (e) {
          console.log('Saving messages from indexes failed:', e);
        }
      }
      return q;
    },

    async messageExists(hash) {
      const r = await knex('Messages').where('hash', hash).count('* as exists');
      return !!parseInt(r[0].exists);
    },

    getMessages(opts) {
      const defaultOptions = {
        orderBy: 'timestamp',
        direction: 'desc',
        limit: 100,
        offset: 0,
        where: {},
      };
      const options = Object.assign(defaultOptions, opts);

      let authorIdentityIdQuery = Promise.resolve([]);
      let recipientIdentityIdQuery = Promise.resolve([]);
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
            default:
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
      return knex.transaction(async (trx) => {
        const r = await trx('MessageAttributes').where({ message_hash: messageHash }).del();
        await trx('Messages').where({ hash: messageHash }).del();
        return !!parseInt(r);
      });
    },

    async getIdentityAttributes(opts) {
      const defaultOptions = {
        orderBy: 'value',
        direction: 'asc',
        limit: 100,
        offset: 0,
        where: {},
        having: {},
        viewpoint: MY_ID,
      };
      const options = Object.assign(defaultOptions, opts);

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

      const r = await knex.from('IdentityAttributes AS attr')
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

      const identities = {};
      let i;
      for (i = 0; i < r.length; i += 1) {
        const attr = r[i];
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

      // Sort by distance
      const arr = Object.values(identities).sort((identity1, identity2) => {
        let smallestDistance1 = 1000;
        let smallestDistance2 = 1000;
        for (i = 0; i < identity1.length; i += 1) {
          if (!Number.isNaN(parseInt(identity1[i].dist)) &&
            identity1[i].dist < smallestDistance1) {
            smallestDistance1 = identity1[i].dist;
          }
        }
        for (i = 0; i < identity2.length; i += 1) {
          if (!Number.isNaN(parseInt(identity2[i].dist)) &&
            identity2[i].dist < smallestDistance2) {
            smallestDistance2 = identity2[i].dist;
          }
        }
        return smallestDistance1 - smallestDistance2;
      });

      return arr;
    },

    async identifierExists(id) {
      const exists = await knex.from('MessageAttributes')
        .where('name', id[0])
        .andWhere('value', id[1])
        .limit(1);
      return !!exists.length;
    },

    async mapIdentityAttributes(opts, forceAdd = false) {
      const options = opts;
      options.viewpoint = options.viewpoint || MY_ID;

      if (!forceAdd) {
        // First see if the identifier exists in any message
        const exists = await pub.identifierExists(options.id);
        if (!exists) {
          return [];
        }
      }

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

      const r = await getExistingId;
      let identityId;
      if (r.length) {
        identityId = parseInt(r[0].identity_id);
        // Delete previously saved attributes of the identity_id
        await knex('IdentityAttributes')
          .where('identity_id', 'in', getExistingId).del();
      } else {
        // No existing identity_id - return a new one
        const rr = await knex('IdentityAttributes')
          .select(knex.raw(`${SQL_IFNULL}(MAX(identity_id), 0) + 1 AS identity_id`));
        identityId = parseInt(rr[0].identity_id);
      }
      // First insert the queried identifier with the identity_id
      await knex('IdentityAttributes').insert({
        identity_id: identityId,
        name: options.id[0],
        value: options.id[1],
        viewpoint_name: options.viewpoint[0],
        viewpoint_value: options.viewpoint[1],
        confirmations: 1,
        refutations: 0,
      });

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
          )
          .groupBy('attr2.name', 'attr2.value');
      }

      async function iterateSearch() {
        await knex('IdentityAttributes').whereIn('identity_id', generateDeleteSubQuery()).del();
        const rr = await knex('IdentityAttributes').insert(generateInsertSubQuery());
        if (JSON.stringify(last) !== JSON.stringify(rr)) {
          last = rr;
          return iterateSearch();
        }
      }

      await iterateSearch();

      const hasSearchedAttributes = options.searchedAttributes &&
        options.searchedAttributes.length > 0;

      if (hasSearchedAttributes) {
        return knex('IdentityAttributes')
          .select('name', 'value as val', 'confirmations as conf', 'refutations as ref')
          .where('identity_id', identityId)
          .whereIn('name', options.searchedAttributes)
          .orderByRaw('confirmations - refutations DESC');
      }

      const ia = await knex('IdentityAttributes')
        .select('name', 'value as val', 'confirmations as conf', 'refutations as ref')
        .where('identity_id', identityId)
        .orderByRaw('confirmations - refutations DESC');

      await pub.getStats(options.id, { viewpoint: options.viewpoint, maxDistance: 0 });
      return ia;
    },

    async getTrustDistance(from, to) {
      if (from[0] === to[0] && from[1] === to[1]) {
        return 0;
      }
      const r = await knex.select('distance').from('TrustDistances').where({
        start_attr_name: from[0],
        start_attr_value: from[1],
        end_attr_name: to[0],
        end_attr_value: to[1],
      });
      return r.length ? r[0].distance : -1;
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
    async generateWebOfTrustIndex(id, maxDepth, maintain, trustedKeyID) {
      if (id[0] !== 'keyID' && !trustedKeyID) {
        throw new Error('Please specify a trusted keyID');
      }
      const trustedKey = trustedKeyID ? ['keyID', trustedKeyID] : id;

      /*
        Can create TrustDistances based on messages authored by startId, or transitively,
        from messages authored by identities that have a TrustDistance from startId
        (when depth > 1)
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

      let q2;
      if (maintain) {
        await this.addTrustIndexedAttribute(id, maxDepth);
      }
      await pub.mapIdentityAttributes({ id, viewpoint: id });
      let i;
      return knex.transaction(async (trx) => {
        await trx('TrustDistances').where({ start_attr_name: id[0], start_attr_value: id[1] }).del();
        await trx('TrustDistances')
          .where({
            start_attr_name: trustedKey[0],
            start_attr_value: trustedKey[1],
          }).del();
        // Add trust distance to self = 0
        await trx('TrustDistances')
          .insert({
            start_attr_name: id[0],
            start_attr_value: id[1],
            end_attr_name: id[0],
            end_attr_value: id[1],
            distance: 0,
          });
        if (trustedKey[0] !== id[0] && trustedKey[1] !== id[1]) {
          await trx('TrustDistances')
            .insert({
              start_attr_name: trustedKey[0],
              start_attr_value: trustedKey[1],
              end_attr_name: trustedKey[0],
              end_attr_value: trustedKey[1],
              distance: 0,
            });
        }
        q2 = Promise.resolve();
        for (i = 1; i <= maxDepth; i += 1) {
          q2.then(buildQuery(true, trx, i));
        }
        await q2;
        q2 = Promise.resolve();
        for (i = 1; i <= maxDepth; i += 1) {
          q2.then(buildQuery(false, trx, i));
        }
        await q2;
        const res = await trx('TrustDistances')
          .where({ start_attr_name: id[0], start_attr_value: id[1] })
          .count('* as wot_size');
        return parseInt(res[0].wot_size);
      });
    },

    async generateIdentityIndex(viewpoint) { // possible param: trustedkeyid
      console.log('Generating identity index (SQL)');

      await pub.mapIdentityAttributes({ id: viewpoint, viewpoint }, true);
      async function mapNextIdentifier() {
        const r = await knex('TrustDistances')
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
          .select('end_attr_name', 'end_attr_value');
        if (r.length) {
          const id = [r[0].end_attr_name, r[0].end_attr_value];
          process.stdout.write('*');
          await pub.mapIdentityAttributes({ id, viewpoint }, true);
          return mapNextIdentifier();
        }
      }

      // for each identifier in WoT: map identity, unless identifier already belongs to an identity
      await knex('IdentityAttributes').where({ viewpoint_name: viewpoint[0], viewpoint_value: viewpoint[1] }).del();
      await knex('IdentityStats').where({ viewpoint_name: viewpoint[0], viewpoint_value: viewpoint[1] }).del();
      await pub.mapIdentityAttributes({ id: viewpoint, viewpoint });
      return mapNextIdentifier();
    },

    async addTrustIndexedAttribute(id, depth) {
      let r = await knex('TrustIndexedAttributes').where({ name: id[0], value: id[1] }).count('* as count');
      if (parseInt(r[0].count)) {
        await knex('TrustIndexedAttributes').where({ name: id[0], value: id[1] }).update({ depth });
      } else {
        await knex('TrustIndexedAttributes').insert({ name: id[0], value: id[1], depth });
        pub.trustIndexedAttributes = await pub.getTrustIndexedAttributes(true);
        r = await knex('TrustDistances')
          .where({
            start_attr_name: id[0],
            start_attr_value: id[1],
            end_attr_name: id[0],
            end_attr_value: id[1],
          })
          .count('* as c');
        if (parseInt(r[0].c) === 0) {
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
      }
    },

    async getTrustIndexedAttributes(forceRefresh) {
      if (pub.trustIndexedAttributes && !forceRefresh) {
        return pub.trustIndexedAttributes;
      }
      const r = await knex('TrustIndexedAttributes').select('*');
      pub.trustIndexedAttributes = r;
      return r;
    },

    async getUniqueIdentifierTypes() {
      const r = await knex('UniqueIdentifierTypes').select('name');
      pub.uniqueIdentifierTypes = [];
      r.forEach((type) => {
        pub.uniqueIdentifierTypes.push(type.name);
      });
    },

    isUniqueType(type) {
      return pub.uniqueIdentifierTypes.indexOf(type) > -1;
    },

    async getMessageCount() {
      const r = await knex('Messages').count('* as count');
      return parseInt(r[0].count);
    },

    async getStats(id, options) {
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

      const sent = knex('Messages as m')
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

      const received = knex('Messages as m')
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
      }
      await identityIdQuery;
      if (!identityId) {
        sent.where({ 'author.name': id[0], 'author.value': id[1] });
        sent.groupBy('author.name', 'author.value');
        sent.select(knex.raw(sentSql));
        received.where({ 'recipient.name': id[0], 'recipient.value': id[1] });
        received.groupBy('recipient.name', 'recipient.value');
        received.select(knex.raw(receivedSql));
      }

      const response = await Promise.all([sent, received]);
      const res = Object.assign({}, response[0][0], response[1][0]);
      Object.keys(res).forEach((key) => {
        if (key.indexOf('sent_') === 0 || key.indexOf('received_') === 0) {
          res[key] = parseInt(res[key]);
        }
      });

      if (options.viewpoint && !options.maxDistance) {
        const identityIds = [];
        if (identityId) {
          identityIds.push(identityId);
        }
        await knex('IdentityStats')
          .where('identity_id', 'in', identityIds)
          .delete();
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
      }

      return res;
    },

    async checkDefaultTrustList() {
      const r = await knex('Messages').count('* as count');
      if (parseInt(r[0].count) === 0) {
        const queries = [];
        const message = Message.createRating({
          author: [MY_ID],
          recipient: [['keyID', '/pbxjXjwEsojbSfdM3wGWfE24F4fX3GasmoHXY3yYPM=']],
          comment: 'Identifi seed node, trusted by default',
          rating: 10,
          context: 'identifi_network',
          public: false,
        });
        const message2 = Message.createRating({
          author: [MY_ID],
          recipient: [['nodeID', 'Qmbb1DRwd75rZk5TotTXJYzDSJL6BaNT1DAQ6VbKcKLhbs']],
          comment: 'Identifi IPFS seed node, trusted by default',
          rating: 10,
          context: 'identifi_network',
          public: false,
        });
        Message.sign(message, MY_KEY.private.pem, MY_KEY.public.hex);
        Message.sign(message2, MY_KEY.private.pem, MY_KEY.public.hex);
        queries.push(this.saveMessage(message));
        queries.push(this.saveMessage(message2));
        return Promise.all(queries);
      }
    },

    async ensureFreeSpace() {
      const r = await this.getMessageCount();
      if (r > config.maxMessageCount) {
        const nMessagesToDelete = Math.min(100, Math.ceil(config.maxMessageCount / 10));
        const messagesToDelete = knex('Messages')
          .select('hash')
          .limit(nMessagesToDelete)
          .orderBy('priority', 'asc')
          .orderBy('created', 'asc');
        await knex('Messages')
          .whereIn('hash', messagesToDelete).del();
        return knex('MessageAttributes')
          .whereIn('message_hash', messagesToDelete)
          .del();
      }
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
    async getPriority(m) {
      const maxPriority = 100;
      const message = m;
      let priority;

      message.signerKeyHash = Message.getSignerKeyHash(message);

      const distanceToSigner = await pub.getTrustDistance(MY_ID, ['keyID', message.signerKeyHash]);
      if (distanceToSigner === -1) { // Unknown signer
        return 0;
      }
      let i;
      const queries = [];
      // Get distances to message authors
      message.signedData.author.forEach(authorId =>
        queries.push(pub.getTrustDistance(MY_ID, authorId)));

      let shortestDistanceToAuthor = 10000000;
      const authorDistances = await Promise.all(queries);
      authorDistances.forEach((d) => {
        if (d > -1 && d < shortestDistanceToAuthor) {
          shortestDistanceToAuthor = d;
        }
      });

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
      return Math.max(priority, 0);
    },

    isLatest() { // param: message
      return true; // TODO: implement
    },

    async saveTrustDistance(startId, endId, distance) {
      await knex('TrustDistances').where({
        start_attr_name: startId[0],
        start_attr_value: startId[1],
        end_attr_name: endId[0],
        end_attr_value: endId[1],
      }).del();
      return knex('TrustDistances').insert({
        start_attr_name: startId[0],
        start_attr_value: startId[1],
        end_attr_name: endId[0],
        end_attr_value: endId[1],
        distance,
      });
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

    async updateIdentityIndexesByMessage(message) { // TODO: param: trustedKeyID
      if (message.signedData.type !== 'verify_identity' && message.signedData.type !== 'unverify_identity') {
        return;
      }

      const queries = [];

      // TODO: make this faster
      const viewpoints = await pub.getTrustIndexedAttributes();
      viewpoints.forEach((vp) => {
        const viewpoint = [vp.name, vp.value];
        message.signedData.recipient.forEach((recipientId) => {
          const q = pub.mapIdentityAttributes({ id: recipientId, viewpoint });
          queries.push(q);
        });
      });
      return Promise.all(queries);

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
            qq.on('td.start_attr_name', '=', knex.raw('?', MY_ID[0]));
            qq.andOn('td.start_attr_value', '=', knex.raw('?', MY_ID[1]));
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

      async function getAndAddHashes(author, recipient) {
        const r = await getHashesQuery(author, recipient);
        return addHashes(r);
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
          const t = author[0] === 'keyID' ? author[1] : MY_KEY.hash; // trusted key
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

    async getIndexesFromIpfsRoot() {
      const res = await knex('TrustIndexedAttributes')
        .where({ name: MY_ID[0], value: MY_ID[1] })
        .whereNotNull('ipfs_index_root')
        .select('ipfs_index_root');
      if (res.length) {
        const links = await p.ipfs.object.links(res[0].ipfs_index_root);
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
        await util.timeoutPromise(Promise.all(queries), 15000);
      }
      p.ipfsIdentitiesBySearchKey = p.ipfsIdentitiesBySearchKey ||
        new btree.MerkleBTree(p.ipfsStorage, 100);
      p.ipfsIdentitiesByDistance = p.ipfsIdentitiesByDistance ||
        new btree.MerkleBTree(p.ipfsStorage, 100);
      p.ipfsMessagesByDistance = p.ipfsMessagesByDistance ||
        new btree.MerkleBTree(p.ipfsStorage, 100);
      p.ipfsMessagesByTimestamp = p.ipfsMessagesByTimestamp ||
        new btree.MerkleBTree(p.ipfsStorage, 100);
    },
  };

  pub.init = async (conf, ipfs) => {
    if (ipfs) {
      p.ipfs = ipfs;
      p.ipfsStorage = new btree.IPFSStorage(p.ipfs);
    }
    config = conf;
    if (conf.db.client === 'pg') {
      SQL_IFNULL = 'COALESCE';
    }
    await schema.init(knex, config);
    await pub.getUniqueIdentifierTypes();
    await pub.addTrustIndexedAttribute(MY_ID, MY_TRUST_INDEX_DEPTH);
    // TODO: if MY_ID is changed, the old one should be removed from TrustIndexedAttributes
    await pub.mapIdentityAttributes({ id: MY_ID });
    await pub.checkDefaultTrustList();
    if (p.ipfsStorage) {
      await p.getIndexesFromIpfsRoot();
    }
    if (p.ipfsStorage) {
      if (process.env.NODE_ENV !== 'test') {
        pub.saveMessagesFromIpfsIndexes(); // non-blocking
      }
      pub.keepAddingNewMessagesToIpfsIndex();
    }
  };

  return pub;
};
