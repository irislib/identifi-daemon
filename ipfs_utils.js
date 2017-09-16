const Message = require('identifi-lib/message');
const btree = require('merkle-btree');
const dagPB = require('ipld-dag-pb');
const crypto = require('crypto');

const util = require('./util');

class IpfsUtils {
  constructor(db) {
    this.db = db;
  }

  async keepAddingNewMessagesToIpfsIndex() {
    await this.db.addNewMessagesToIpfsIndex();
    return new Promise(((resolve) => {
      setTimeout(() => {
        resolve(this.keepAddingNewMessagesToIpfsIndex());
      }, 10000);
    }));
  }

  async addIndexesToIpfs() {
    await this.addMessageIndexToIpfs();
    return this.addIdentityIndexToIpfs();
  }

  async addMessageIndexToIpfs() {
    const maxMsgCount = 100000;
    let msgsToIndex = [];
    const iterate = async (limit = 10000, initialOffset = 0, initialDistance = 0) => {
      let distance = initialDistance;
      let offset = initialOffset;
      const msgs = await this.db.getMessages({
        limit,
        offset,
        orderBy: 'timestamp',
        direction: 'asc',
        viewpoint: this.db.MY_ID,
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
        const key = this.constructor.getMsgIndexKey(msg);
        msgsToIndex.push({ key, value: msg, targetHash: null });
      });
      const hasMore = !(msgs.length === 0 && offset === 0);
      if (msgsToIndex.length < maxMsgCount && hasMore) {
        return iterate(limit, offset, distance);
      }
      return msgsToIndex.length;
    };

    console.log('adding msgs to ipfs');
    const r = await iterate();
    console.log('res', r);
    console.log('adding messages_by_distance index to ipfs');
    this.db.ipfsMessagesByDistance = await btree.MerkleBTree.fromSortedList(
      msgsToIndex.slice(),
      this.db.IPFS_INDEX_WIDTH,
      this.db.ipfsStorage,
    );

    // create index of messages sorted by timestamp
    msgsToIndex.forEach((msg) => {
      msg.key = msg.key.substr(msg.key.indexOf(':') + 1); // eslint-disable-line no-param-reassign
    });
    msgsToIndex = msgsToIndex.sort(util.sortByKey);
    console.log('adding messages_by_timestamp index to ipfs');
    this.db.ipfsMessagesByTimestamp = await btree.MerkleBTree.fromSortedList(
      msgsToIndex,
      this.db.IPFS_INDEX_WIDTH,
      this.db.ipfsStorage,
    );
    // Add message index to IPFS
    return this.addIndexRootToIpfs();
  }

  async saveMessageFromIpfs(path) {
    const r = await this.db.knex('Messages').where('ipfs_hash', path).count('* as count');
    if (parseInt(r[0].count) === 0) {
      try {
        const buffer = await util.timeoutPromise(this.db.ipfs.files
          .cat(path, { buffer: true }), 5000);
        if (buffer) {
          const msg = { jws: buffer.toString('utf8'), ipfs_hash: path };
          process.stdout.write('+');
          Message.verify(msg);
          console.log('saving new msg from ipfs:', msg.ipfs_hash);
          return this.db.saveMessage(msg);
        }
        process.stdout.write('-');
      } catch (e) {
        console.log('Processing message', path, 'failed:', e);
      }
    }
  }

  async saveMessagesFromIpfsIndex(ipnsName) {
    try {
      console.log('Getting path for name', ipnsName);
      if (!(this.db.ipfs && this.db.ipfs.name)) {
        console.log('ipfs.name is not available');
        return;
      }
      const r = await util.timeoutPromise(this.db.ipfs.name.resolve(ipnsName), 60000);
      if (!r) { throw new Error('Ipfs index name was not resolved', ipnsName); }
      let path = r.Path.replace('/ipfs/', '');
      console.log('resolved name', path);
      const links = await util.timeoutPromise(this.db.ipfs.object.links(path), 30000);
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
      const index = await btree.MerkleBTree
        .getByHash(path, this.db.ipfsStorage, this.db.IPFS_INDEX_WIDTH);
      const msgs = await index.searchText('', 100000);
      let q = Promise.resolve();
      console.log('Processing', msgs.length, 'messages from index');
      msgs.forEach((entry) => {
        const msg = { jws: entry.value.jws };
        if (Message.decode(msg)) {
          process.stdout.write('.');
          q = q.then(() => this.db.saveMessage(msg, false, false));
        }
      });
      await q;
      console.log('Finished saving messages from index', ipnsName);
      return this.addDbMessagesToIpfs();
    } catch (e) {
      console.log('Processing index', ipnsName, 'failed:', e);
    }
  }

  async saveMessagesFromIpfsIndexes() {
    const r = await this.db.knex('Messages')
      .innerJoin('MessageAttributes as author', (q) => {
        q.on('author.message_hash', '=', 'Messages.hash');
        q.andOn('author.is_recipient', '=', this.db.knex.raw('?', false));
      })
      .innerJoin('MessageAttributes as recipient', (q) => {
        q.on('recipient.message_hash', '=', 'Messages.hash');
        q.andOn('recipient.is_recipient', '=', this.db.knex.raw('?', true));
      })
      .innerJoin('TrustDistances as td', (q) => {
        q.on('td.start_attr_name', '=', this.db.knex.raw('?', this.db.MY_ID[0]));
        q.on('td.start_attr_value', '=', this.db.knex.raw('?', this.db.MY_ID[1]));
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
        q = q.then(this.saveMessagesFromIpfsIndex(r[i].value));
      } catch (e) {
        console.log('Saving messages from indexes failed:', e);
      }
    }
    return q;
  }

  async addMessageToIpfsIndex(msg) {
    try { // TODO: move these try/catches to calling function
      const message = msg;
      const msgIndexKey = this.constructor.getMsgIndexKey(message); // TODO: should have distance
      await this.db.ipfsMessagesByDistance.put(msgIndexKey, message);
      await this.db.ipfsMessagesByTimestamp.put(msgIndexKey.substr(msgIndexKey.indexOf(':') + 1), message);
      const authorAttrs = await this.db.getIdentityAttributesByAuthorOrRecipient(message, true);
      await this.addIdentityToIpfsIndex(authorAttrs);

      const recipientAttrs = await this.db.getIdentityAttributesByAuthorOrRecipient(message, false);
      let shortestDistance = 99;
      recipientAttrs.forEach((attr) => {
        if (typeof attr.dist === 'number' && attr.dist < shortestDistance) {
          shortestDistance = attr.dist;
        }
      });
      if (shortestDistance < 99) {
        message.distance = shortestDistance;
      }
      return this.addIdentityToIpfsIndex(recipientAttrs);
    } catch (e) {
      console.log('adding to ipfs failed:', e);
    }
  }

  addMessageToIpfs(message) {
    return this.db.ipfs.files.add(Buffer.from(message.jws, 'utf8'));
  }

  async getReindexQuery(counter) {
    const msgs = {};
    let r = await this.db.knex('Messages').whereNull('ipfs_hash').select('jws', 'hash').limit(100);
    if (r.length && this.db.ipfs) {
      r.forEach((msg) => {
        msgs[msg.hash] = Buffer.from(msg.jws, 'utf8');
      });
      try {
        r = await this.db.ipfs.files.add(Object.values(msgs));
      } catch (e) {
        console.log('adding to ipfs failed:', e);
      }
    } else {
      r = [];
    }
    console.log('added', r.length, 'msgs to ipfs');
    const queries = [];
    Object.keys(msgs).forEach((hash, index) => {
      queries.push(this.db.knex('Messages').where({ hash }).update({ ipfs_hash: r[index].hash }));
    });
    r = await Promise.all(queries);
    console.log('updated', r.length, 'db entries');
    if (r.length) {
      return this.getReindexQuery(counter + 1);
    }
    return counter;
  }

  async addDbMessagesToIpfs() {
    const c = await this.getReindexQuery(0);
    return `Reindexed ${c} messages`;
  }

  async addIndexRootToIpfs() {
    // saves indexes as an IPFS directory
    // TODO: if info already exists, don't rewrite
    try {
      let indexRoot;
      const a = await this.db.getIdentityAttributes({ id: this.db.MY_ID });
      let attrs = a;
      if (attrs.length) {
        attrs = attrs[0];
      } else {
        attrs = [];
      }
      let res = await this.db.ipfs.files.add([
        { path: 'info', content: Buffer.from(JSON.stringify({ keyID: this.db.MY_ID[1], attrs })) },
        { path: 'messages_by_distance', content: Buffer.from(this.db.ipfsMessagesByDistance.rootNode.serialize()) },
        { path: 'messages_by_timestamp', content: Buffer.from(this.db.ipfsMessagesByTimestamp.rootNode.serialize()) },
        { path: 'identities_by_searchkey', content: Buffer.from(this.db.ipfsIdentitiesBySearchKey.rootNode.serialize()) },
        { path: 'identities_by_distance', content: Buffer.from(this.db.ipfsIdentitiesByDistance.rootNode.serialize()) },
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
          resolve(this.db.ipfs.object.put(dag));
        });
      }));
      if (res._json.multihash) {
        indexRoot = res._json.multihash;
        return this.db.knex('TrustIndexedAttributes')
          .where({ name: this.db.MY_ID[0], value: this.db.MY_ID[1] })
          .update('ipfs_index_root', res._json.multihash)
          .return(res);
      }
      res = await Promise.resolve(res);
      if (this.db.ipfs.name && res._json.multihash) {
        console.log('publishing index', res._json.multihash);
        const r = await this.db.ipfs.name.publish(res._json.multihash, {});
        console.log('published index', r);
      }
      return indexRoot;
    } catch (e) {
      console.log('error publishing index', e);
    }
  }

  static getIdentityProfileIndexKeys(identityProfile, hash) {
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
  }

  async addIdentityToIpfsIndex(attrs) {
    const ip = await this.db.getIdentityProfile(attrs);
    console.log('adding identityprofile to ipfs', ip);
    const r = await this.db.ipfs.files.add(Buffer.from(JSON.stringify(ip), 'utf8'));
    if (r.length) {
      const hash = crypto.createHash('md5').update(JSON.stringify(ip)).digest('base64');
      let q = Promise.resolve();
      let q2 = Promise.resolve();
      this.constructor.getIdentityProfileIndexKeys(ip, hash).forEach((key) => {
        console.log('adding key', key);
        console.log('and', key.substr(key.indexOf(':') + 1));
        q = q.then(this.db.ipfsIdentitiesByDistance.put(key, r[0].hash));
        q2 = q2.then(this.db.ipfsIdentitiesBySearchKey.put(key.substr(key.indexOf(':') + 1), r[0].hash));
      });
      return util.timeoutPromise(Promise.all([q, q2]), 30000);
    }
  }

  async getIndexKeysByIdentity(attrs) {
    const identityProfile = await this.db.getIdentityProfile(attrs);
    const keys = [];
    const hash = crypto.createHash('md5').update(JSON.stringify(identityProfile)).digest('base64');
    this.constructor.getIdentityProfileIndexKeys(identityProfile, hash).forEach((key) => {
      keys.push(key);
      keys.push(key.substr(key.indexOf(':') + 1));
    });
    return keys;
  }

  async addIdentityIndexToIpfs() {
    const maxIndexSize = 100000;
    const identityIndexEntriesToAdd = [];
    const identityProfilesByHash = {};
    const r = await this.db.getIdentityAttributes({ limit: maxIndexSize });
    console.log('Adding identity index of', r.length, 'entries to ipfs');

    const iterate = async (i) => {
      console.log(`${i}/${r.length}`);
      if (i >= r.length) {
        return;
      }
      const identityProfile = await this.db.getIdentityProfile(r[i]);
      const hash = crypto.createHash('md5').update(JSON.stringify(identityProfile)).digest('base64');
      identityProfilesByHash[hash] = identityProfile;
      this.constructor.getIdentityProfileIndexKeys(identityProfile, hash).forEach((key) => {
        identityIndexEntriesToAdd.push({ key, value: hash, targetHash: null });
      });
      return iterate(i + 1);
    };

    await iterate(0);
    const orderedKeys = Object.keys(identityProfilesByHash);

    const addIdentityProfilesToIpfs = async () => {
      if (!orderedKeys.length) {
        return;
      }
      const keys = orderedKeys.splice(0, 100);
      const values = [];
      keys.forEach((key) => {
        values.push(Buffer.from(JSON.stringify(identityProfilesByHash[key]), 'utf8'));
      });
      const rr = await this.db.ipfs.files.add(values);
      keys.forEach((key, i) => {
        if (i < rr.length && rr[i].hash) {
          identityProfilesByHash[key] = rr[i].hash;
        }
      });
      return addIdentityProfilesToIpfs();
    };

    await addIdentityProfilesToIpfs();
    identityIndexEntriesToAdd.forEach((entry) => {
      // eslint-disable-next-line no-param-reassign
      entry.value = identityProfilesByHash[entry.value];
    });

    console.log('building index identities_by_distance');
    this.db.ipfsIdentitiesByDistance = await btree.MerkleBTree.fromSortedList(
      identityIndexEntriesToAdd.sort(util.sortByKey).slice(),
      this.db.IPFS_INDEX_WIDTH,
      this.db.ipfsStorage,
    );
    identityIndexEntriesToAdd.forEach((entry) => {
      entry.key = entry.key.substr(entry.key.indexOf(':') + 1); // eslint-disable-line no-param-reassign
    });
    console.log('building index identities_by_searchkey');
    this.db.ipfsIdentitiesBySearchKey = await btree.MerkleBTree.fromSortedList(
      identityIndexEntriesToAdd.sort(util.sortByKey),
      this.db.IPFS_INDEX_WIDTH,
      this.db.ipfsStorage,
    );
    return this.addIndexRootToIpfs();
  }

  static getMsgIndexKey(msg) {
    let distance = parseInt(msg.distance);
    distance = Number.isNaN(distance) ? 99 : distance;
    distance = (`00${distance}`).substring(distance.toString().length); // pad with zeros
    const key = `${distance}:${Math.floor(Date.parse(msg.timestamp || msg.signedData.timestamp) / 1000)}:${(msg.ipfs_hash || msg.hash).substr(0, 9)}`;
    return key;
  }

  async getIndexesFromIpfsRoot() {
    const res = await this.db.knex('TrustIndexedAttributes')
      .where({ name: this.db.MY_ID[0], value: this.db.MY_ID[1] })
      .whereNotNull('ipfs_index_root')
      .select('ipfs_index_root');
    if (res.length) {
      const links = await this.db.ipfs.object.links(res[0].ipfs_index_root);
      const queries = [];
      links.forEach((link) => {
        switch (link._name) {
          case 'messages_by_distance':
            queries.push(btree.MerkleBTree.getByHash(link._multihash, this.db.ipfsStorage)
              .then((index) => {
                this.db.ipfsMessagesByDistance = index;
              }));
            break;
          case 'messages_by_timestamp':
            queries.push(btree.MerkleBTree.getByHash(link._multihash, this.db.ipfsStorage)
              .then((index) => {
                this.db.ipfsMessagesByTimestamp = index;
              }));
            break;
          case 'identities_by_distance':
            queries.push(btree.MerkleBTree.getByHash(link._multihash, this.db.ipfsStorage)
              .then((index) => {
                this.db.ipfsIdentitiesByDistance = index;
              }));
            break;
          case 'identities_by_searchkey':
            queries.push(btree.MerkleBTree.getByHash(link._multihash, this.db.ipfsStorage)
              .then((index) => {
                this.db.ipfsIdentitiesBySearchKey = index;
              }));
            break;
          default:
            break;
        }
      });
      await util.timeoutPromise(Promise.all(queries), 15000);
    }
    this.db.ipfsIdentitiesBySearchKey = this.db.ipfsIdentitiesBySearchKey ||
      new btree.MerkleBTree(this.db.ipfsStorage, 100);
    this.db.ipfsIdentitiesByDistance = this.db.ipfsIdentitiesByDistance ||
      new btree.MerkleBTree(this.db.ipfsStorage, 100);
    this.db.ipfsMessagesByDistance = this.db.ipfsMessagesByDistance ||
      new btree.MerkleBTree(this.db.ipfsStorage, 100);
    this.db.ipfsMessagesByTimestamp = this.db.ipfsMessagesByTimestamp ||
      new btree.MerkleBTree(this.db.ipfsStorage, 100);
  }
}

module.exports = IpfsUtils;
