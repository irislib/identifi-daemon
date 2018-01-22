const Message = require('identifi-lib/message');
const keyutil = require('identifi-lib/keyutil');
const btree = require('merkle-btree');

const util = require('./util');
const IpfsUtils = require('./ipfs_utils.js');
const schema = require('./schema');

const MY_TRUST_INDEX_DEPTH = 4;
const IPFS_INDEX_WIDTH = 200;
const REBUILD_INDEXES_IF_NEW_MSGS_GT = 30;

class IdentifiDB {
  constructor(knex) {
    this.ipfsUtils = new IpfsUtils(this); // eslint-disable-line new-cap
    this.knex = knex;
    this.lastIpfsIndexedMessageSavedAt = (new Date()).toISOString();
    this.MY_KEY = keyutil.getDefault();
    this.MY_ID = ['keyID', this.MY_KEY.hash];
    this.SQL_IFNULL = 'IFNULL';
    this.IPFS_INDEX_WIDTH = IPFS_INDEX_WIDTH;
    this.ipfsIdentityIndexKeysToRemove = {};
    this.IndexedViewpoints = null;
  }

  async saveMessage(msg, updateTrustIndexes = true, addToIpfs = true) {
    const message = msg;
    if (typeof message.signerKeyHash === 'undefined') {
      message.signerKeyHash = Message.getSignerKeyHash(message);
    }
    await this.ensureFreeSpace();
    // Unobtrusively store msg to ipfs
    if ((this.ipfs && !message.ipfs_hash) && addToIpfs) {
      const res = await this.ipfsUtils.addMessageToIpfs(message);
      message.ipfs_hash = res[0].hash;
    }
    const exists = await this.messageExists(message.hash);

    if (exists) {
      if (addToIpfs && message.ipfs_hash) {
      // Msg was added to IPFS - update ipfs_hash
        return this.knex('Messages').where({ hash: message.hash }).update({ ipfs_hash: message.ipfs_hash });
      }
      return false;
    }
    if (Object.keys(this.ipfsIdentityIndexKeysToRemove).length < REBUILD_INDEXES_IF_NEW_MSGS_GT) {
      // Mark for deletion the index references to expiring identity profiles
      this.ipfsIdentityIndexKeysToRemove[message.hash] = [];
      const authorAttrs = await this.getIdentityAttributesByAuthorOrRecipient(message, true);
      const authorKeys = await this.ipfsUtils.getIndexKeysByIdentity(authorAttrs);
      this.ipfsIdentityIndexKeysToRemove[message.hash] =
        this.ipfsIdentityIndexKeysToRemove[message.hash].concat(authorKeys);
      const recipientAttrs = this.getIdentityAttributesByAuthorOrRecipient(message, false);
      const recipientKeys = await this.ipfsUtils.getIndexKeysByIdentity(recipientAttrs);
      this.ipfsIdentityIndexKeysToRemove[message.hash] =
        this.ipfsIdentityIndexKeysToRemove[message.hash].concat(recipientKeys);
    }
    const isPublic = typeof message.signedData.public === 'undefined' ? true : message.signedData.public;
    await this.deletePreviousMessage(message);
    const priority = await this.getPriority(message);
    await this.knex.transaction(async (trx) => {
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
        is_latest: this.constructor.isLatest(message),
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
      await this.updateWotIndexesByMessage(message);
      await this.updateIdentityIndexesByMessage(message);
    }
    return message;
  }

  async addNewMessagesToIpfsIndex() {
    try {
      const messages = await this.getMessages({
        limit: 10000,
        orderBy: 'timestamp',
        direction: 'desc',
        viewpoint: this.MY_ID,
        savedAtGt: this.lastIpfsIndexedMessageSavedAt,
      });
      if (messages.length) {
        console.log('', messages.length, 'new messages to index');
      } else {
        return;
      }
      /* rebuilding the indexes is more efficient than
      inserting large number of entries individually */
      if (messages.length < REBUILD_INDEXES_IF_NEW_MSGS_GT) {
        let q = Promise.resolve();
        // remove identity index entries that point to expired identity profiles
        Object.keys(this.ipfsIdentityIndexKeysToRemove).forEach((msg) => {
          this.ipfsIdentityIndexKeysToRemove[msg].forEach((key) => {
            q = q.then(() => {
              const q2 = this.ipfsIdentitiesByDistance.delete(key);
              const q3 = this.ipfsIdentitiesBySearchKey.delete(key.substr(key.indexOf(':') + 1));
              return util.timeoutPromise(Promise.all([q2, q3]), 30000);
            }).then(() => console.log('deleted old entry', key));
            delete this.ipfsIdentityIndexKeysToRemove[msg];
          });
        });
        messages.forEach((msg) => {
          const message = Message.decode(msg);
          const d = new Date(message.saved_at).toISOString();
          if (d > this.lastIpfsIndexedMessageSavedAt) {
            this.lastIpfsIndexedMessageSavedAt = d;
          }
          q = q.then(() => this.ipfsUtils.addMessageToIpfsIndex(message));
        });
        const r = await util.timeoutPromise(q, 200000);
        if (typeof r === 'undefined') {
          this.ipfsUtils.addIndexesToIpfs();
        }
      }
      this.ipfsIdentityIndexKeysToRemove = {};
      if (messages.length) {
        console.log('adding index root to ipfs');
        return this.ipfsUtils.addIndexRootToIpfs();
      }
    } catch (e) {
      console.log('adding new messages to ipfs failed:', e);
    }
  }

  getIdentityAttributesByAuthorOrRecipient(message, getByAuthor, limit) {
    // pick first unique author or recipient attribute from message
    const attributes = getByAuthor ? message.signedData.author : message.signedData.recipient;
    for (let i = 0; i < attributes.length; i += 1) {
      if (this.isUniqueType(attributes[i][0])) {
        return this.getIdentityAttributes({ limit: limit || 10, id: attributes[i] })
          .then(attrs => (attrs.length ? attrs[0] : []));
      }
    }
    return [];
  }

  async getIdentityStats(uniqueAttr, viewpoint) {
    return this.knex.from('IdentityAttributes').where({
      name: uniqueAttr.name || uniqueAttr[0],
      type: uniqueAttr.value || uniqueAttr[1],
      viewpoint_name: viewpoint[0],
      viewpoint_type: viewpoint[1],
    })
      .innerJoin('IdentityStats', 'IdentityAttributes.identity_id', 'IdentityStats.identity_id')
      .select('*');
  }

  async getIdentityProfile(attrs, useCache = false) {
    const identityProfile = { attrs };
    if (!attrs.length) {
      return identityProfile;
    }
    let uniqueAttr = attrs[0];
    for (let i = 0; i < attrs.length; i += 1) {
      if (this.isUniqueType(attrs[i].name)) {
        uniqueAttr = attrs[i];
      }
    }
    uniqueAttr = [uniqueAttr.name, uniqueAttr.val];

    const identityIdQuery = this.getIdentityID(uniqueAttr, this.MY_ID);

    if (useCache) {
      const res = await this.knex.from('IdentityStats')
        .select('cached_identity_profile').whereIn('identity_id', identityIdQuery);
      if (res.length > 0 && res[0].cached_identity_profile &&
          res[0].cached_identity_profile.length > 0) {
        return JSON.parse(res[0].cached_identity_profile);
      }
    }

    const received = await this.getMessages({
      recipient: uniqueAttr,
      limit: 10000,
      orderBy: 'timestamp',
      direction: 'asc',
      viewpoint: this.MY_ID,
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
      if (msgs.length && this.ipfsStorage) {
        const receivedIndex = await btree.MerkleBTree
          .fromSortedList(msgs, IPFS_INDEX_WIDTH, this.ipfsStorage);
        identityProfile.received = receivedIndex.rootNode.hash;
      }
      const sent = await this.getMessages({
        author: uniqueAttr,
        limit: 10000,
        orderBy: 'timestamp',
        direction: 'asc',
        viewpoint: this.MY_ID,
      });
      msgs = [];
      sent.forEach((msg) => {
        msgs.push({
          key: `${Date.parse(msg.timestamp)}:${(msg.ipfs_hash || msg.hash).substr(0, 9)}`,
          value: { jws: msg.jws },
          targetHash: null,
        });
      });
      if (msgs.length && this.ipfsStorage) {
        const sentIndex = await btree.MerkleBTree
          .fromSortedList(msgs, IPFS_INDEX_WIDTH, this.ipfsStorage);
        identityProfile.sent = sentIndex.rootNode.hash;
      }
    } catch (e) {
      console.log('adding', attrs, 'failed:', e);
    }
    await this.knex('IdentityStats')
      .update('cached_identity_profile', JSON.stringify(identityProfile))
      .whereIn('identity_id', identityIdQuery);
    return identityProfile;
  }

  async messageExists(hash) {
    const r = await this.knex('Messages').where('hash', hash).count('* as exists');
    return !!parseInt(r[0].exists);
  }

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
        authorIdentityIdQuery = this.knex('IdentityAttributes')
          .where({
            name: options.author[0],
            value: options.author[1],
            viewpoint_name: options.viewpoint[0],
            viewpoint_value: options.viewpoint[1],
          })
          .select('identity_id');
      }

      if (options.recipient) {
        recipientIdentityIdQuery = this.knex('IdentityAttributes')
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
        select.push(this.knex.raw('MIN(td.distance) AS "distance"'));
        select.push(this.knex.raw('MAX(st.positive_score) AS author_pos'));
        select.push(this.knex.raw('MAX(st.negative_score) AS author_neg'));
      }
      const query = this.knex.select(select)
        .groupBy('Messages.hash')
        .from('Messages')
        .innerJoin('MessageAttributes as author', (q) => {
          q.on('Messages.hash', '=', 'author.message_hash');
          q.on('author.is_recipient', '=', this.knex.raw('?', false));
        })
        .innerJoin('MessageAttributes as recipient', (q) => {
          q.on('Messages.hash', '=', 'recipient.message_hash');
          q.andOn('recipient.is_recipient', '=', this.knex.raw('?', true));
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
          q.on('author_attribute.viewpoint_name', '=', this.knex.raw('?', options.viewpoint[0]));
          q.on('author_attribute.viewpoint_value', '=', this.knex.raw('?', options.viewpoint[1]));
        });
        query.leftJoin('IdentityStats as st', 'st.identity_id', 'author_attribute.identity_id');
        query.leftJoin('IdentityAttributes as recipient_attribute', (q) => {
          q.on('recipient_attribute.name', '=', 'recipient.name');
          q.on('recipient_attribute.value', '=', 'recipient.value');
          q.on('recipient_attribute.viewpoint_name', '=', this.knex.raw('?', options.viewpoint[0]));
          q.on('recipient_attribute.viewpoint_value', '=', this.knex.raw('?', options.viewpoint[1]));
        });
        query.innerJoin('TrustDistances as td', (q) => {
          q.on('author_attribute.name', '=', 'td.end_attr_name')
            .andOn('author_attribute.value', '=', 'td.end_attr_value')
            .andOn('td.start_attr_name', '=', this.knex.raw('?', options.viewpoint[0]))
            .andOn('td.start_attr_value', '=', this.knex.raw('?', options.viewpoint[1]));
          if (options.maxDistance > 0) {
            q.andOn('td.distance', '<=', this.knex.raw('?', options.maxDistance));
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
  }

  dropMessage(messageHash) {
    return this.knex.transaction(async (trx) => {
      const r = await trx('MessageAttributes').where({ message_hash: messageHash }).del();
      await trx('Messages').where({ hash: messageHash }).del();
      return !!parseInt(r);
    });
  }

  async getIdentityAttributes(opts) {
    const defaultOptions = {
      orderBy: 'value',
      direction: 'asc',
      limit: 100,
      offset: 0,
      where: {},
      having: {},
      viewpoint: this.MY_ID,
    };
    const options = Object.assign(defaultOptions, opts);

    if (options.id) {
      options.where['attr.name'] = options.id[0];
      options.where['attr.value'] = options.id[1];
    }

    options.where['attr.viewpoint_name'] = options.viewpoint[0];
    options.where['attr.viewpoint_value'] = options.viewpoint[1];

    const subquery = this.knex.from('IdentityAttributes AS attr2')
      .select(this.knex.raw('attr.identity_id'))
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
      .orderBy(this.knex.raw(`MIN(${this.SQL_IFNULL}(td.distance, 100000))`), options.direction)
      .limit(options.limit)
      .offset(options.offset);

    if (options.searchValue) {
      subquery.where(this.knex.raw('lower("attr"."value")'), 'LIKE', `%${options.searchValue.toLowerCase()}%`);
    }

    const r = await this.knex.from('IdentityAttributes AS attr')
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
  }

  async identifierExists(id) {
    const exists = await this.knex.from('MessageAttributes')
      .where('name', id[0])
      .andWhere('value', id[1])
      .limit(1);
    return !!exists.length;
  }

  async mapIdentityAttributes(opts, forceAdd = false) {
    const options = opts;
    options.viewpoint = options.viewpoint || this.MY_ID;

    if (!forceAdd) {
      // First see if the identifier exists in any message
      const exists = await this.identifierExists(options.id);
      if (!exists) {
        return [];
      }
    }

    // Find out existing identity_id for the identifier
    const getExistingId = this.knex.from('IdentityAttributes as ia')
      .select('identity_id')
      .where({
        'ia.name': options.id[0],
        'ia.value': options.id[1],
        viewpoint_name: options.viewpoint[0],
        viewpoint_value: options.viewpoint[1],
      })
      .innerJoin('UniqueAttributeTypes as uidt', 'uidt.name', 'ia.name');

    const r = await getExistingId;
    let identityId;
    if (r.length) {
      identityId = parseInt(r[0].identity_id);
      // Delete previously saved attributes of the identity_id
      await this.knex('IdentityAttributes')
        .where('identity_id', 'in', getExistingId).del();
    } else {
      // No existing identity_id - return a new one
      const rr = await this.knex('IdentityAttributes')
        .select(this.knex.raw(`${this.SQL_IFNULL}(MAX(identity_id), 0) + 1 AS identity_id`));
      identityId = parseInt(rr[0].identity_id);
    }
    // First insert the queried identifier with the identity_id
    await this.knex('IdentityAttributes').insert({
      identity_id: identityId,
      name: options.id[0],
      value: options.id[1],
      viewpoint_name: options.viewpoint[0],
      viewpoint_value: options.viewpoint[1],
      confirmations: 1,
      refutations: 0,
    });

    let last;
    const generateSubQuery = () =>
      this.knex
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
        .innerJoin('UniqueAttributeTypes as uidt', 'uidt.name', 'attr1.name')
        .innerJoin('TrustDistances as td_signer', (q) => {
          q.on('td_signer.start_attr_name', '=', this.knex.raw('?', options.viewpoint[0]));
          q.on('td_signer.start_attr_value', '=', this.knex.raw('?', options.viewpoint[1]));
          q.on('td_signer.end_attr_name', '=', this.knex.raw('?', 'keyID'));
          q.on('td_signer.end_attr_value', '=', 'm.signer_keyid');
        });

    const generateDeleteSubQuery = () =>
      generateSubQuery()
        // Select for deletion the related identity attributes that were previously inserted
        // with a different identity_id
        .innerJoin('IdentityAttributes as existing', (q) => {
          q.on('existing.identity_id', '!=', identityId);
          q.on('existing.name', '=', 'attr2.name');
          q.on('existing.value', '=', 'attr2.value');
          q.on('existing.viewpoint_name', '=', this.knex.raw('?', options.viewpoint[0]));
          q.on('existing.viewpoint_value', '=', this.knex.raw('?', options.viewpoint[1]));
        })
        .innerJoin('UniqueAttributeTypes as uidt2', 'uidt2.name', 'existing.name')
        .select('existing.identity_id');

    const generateInsertSubQuery = () =>
      generateSubQuery()
        // Select for insertion the related identity attributes that do not already exist
        // on the identity_id
        .leftJoin('IdentityAttributes as existing', (q) => {
          q.on('existing.identity_id', '=', identityId);
          q.on('existing.name', '=', 'attr2.name');
          q.on('existing.value', '=', 'attr2.value');
          q.on('existing.viewpoint_name', '=', this.knex.raw('?', options.viewpoint[0]));
          q.on('existing.viewpoint_value', '=', this.knex.raw('?', options.viewpoint[1]));
        })
        .whereNull('existing.identity_id')
        .select(
          identityId,
          'attr2.name',
          'attr2.value',
          this.knex.raw('?', options.viewpoint[0]),
          this.knex.raw('?', options.viewpoint[1]),
          this.knex.raw('SUM(CASE WHEN m.type = \'verify_identity\' THEN 1 ELSE 0 END)'),
          this.knex.raw('SUM(CASE WHEN m.type = \'unverify_identity\' THEN 1 ELSE 0 END)'),
        )
        .groupBy('attr2.name', 'attr2.value');

    const iterateSearch = async () => {
      await this.knex('IdentityAttributes').whereIn('identity_id', generateDeleteSubQuery()).del();
      const rr = await this.knex('IdentityAttributes').insert(generateInsertSubQuery());
      if (JSON.stringify(last) !== JSON.stringify(rr)) {
        last = rr;
        return iterateSearch();
      }
    };

    await iterateSearch();

    const hasSearchedAttributes = options.searchedAttributes &&
      options.searchedAttributes.length > 0;

    if (hasSearchedAttributes) {
      return this.knex('IdentityAttributes')
        .select('name', 'value as val', 'confirmations as conf', 'refutations as ref')
        .where('identity_id', identityId)
        .whereIn('name', options.searchedAttributes)
        .orderByRaw('confirmations - refutations DESC');
    }

    const ia = await this.knex('IdentityAttributes')
      .select('name', 'value as val', 'confirmations as conf', 'refutations as ref')
      .where('identity_id', identityId)
      .orderByRaw('confirmations - refutations DESC');

    await this.getStats(options.id, { viewpoint: options.viewpoint, maxDistance: 0 });
    return ia;
  }

  async getTrustDistance(from, to) {
    if (from[0] === to[0] && from[1] === to[1]) {
      return 0;
    }
    const r = await this.knex.select('distance').from('TrustDistances').where({
      start_attr_name: from[0],
      start_attr_value: from[1],
      end_attr_name: to[0],
      end_attr_value: to[1],
    });
    return r.length ? r[0].distance : -1;
  }

  getTrustDistances(from) {
    return this.knex.select('*').from('TrustDistances').where({
      start_attr_name: from[0],
      start_attr_value: from[1],
    });
  }

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
    const buildQuery = (betweenKeyIDsOnly, trx, depth) => {
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
        subQuery.innerJoin('UniqueAttributeTypes as uidt1', 'uidt1.name', 'attr1.name');
        subQuery.innerJoin('UniqueAttributeTypes as uidt2', 'uidt2.name', 'attr2.name');
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
    };

    let q2;
    if (maintain) {
      await this.addTrustIndexedAttribute(id, maxDepth);
    }
    await this.mapIdentityAttributes({ id, viewpoint: id });
    let i;
    return this.knex.transaction(async (trx) => {
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
  }

  async generateIdentityIndex(viewpoint) { // possible param: trustedkeyid
    console.log('Generating identity index (SQL)');

    await this.mapIdentityAttributes({ id: viewpoint, viewpoint }, true);
    const mapNextIdentifier = async () => {
      const r = await this.knex('TrustDistances')
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
        await this.mapIdentityAttributes({ id, viewpoint }, true);
        return mapNextIdentifier();
      }
    };

    // for each identifier in WoT: map identity, unless identifier already belongs to an identity
    await this.knex('IdentityAttributes').where({ viewpoint_name: viewpoint[0], viewpoint_value: viewpoint[1] }).del();
    await this.knex('IdentityStats').where({ viewpoint_name: viewpoint[0], viewpoint_value: viewpoint[1] }).del();
    await this.mapIdentityAttributes({ id: viewpoint, viewpoint });
    return mapNextIdentifier();
  }

  async addTrustIndexedAttribute(id, depth) {
    let r = await this.knex('IndexedViewpoints').where({ name: id[0], value: id[1] }).count('* as count');
    if (parseInt(r[0].count)) {
      await this.knex('IndexedViewpoints').where({ name: id[0], value: id[1] }).update({ depth });
    } else {
      await this.knex('IndexedViewpoints').insert({ name: id[0], value: id[1], depth });
      this.IndexedViewpoints = await this.getIndexedViewpoints(true);
      r = await this.knex('TrustDistances')
        .where({
          start_attr_name: id[0],
          start_attr_value: id[1],
          end_attr_name: id[0],
          end_attr_value: id[1],
        })
        .count('* as c');
      if (parseInt(r[0].c) === 0) {
      // Add trust distance to self = 0
        return this.knex('TrustDistances')
          .insert({
            start_attr_name: id[0],
            start_attr_value: id[1],
            end_attr_name: id[0],
            end_attr_value: id[1],
            distance: 0,
          }).return();
      }
    }
  }

  async getIndexedViewpoints(forceRefresh) {
    if (this.IndexedViewpoints && !forceRefresh) {
      return this.IndexedViewpoints;
    }
    const r = await this.knex('IndexedViewpoints').select('*');
    this.IndexedViewpoints = r;
    return r;
  }

  async getUniqueAttributeTypes() {
    const r = await this.knex('UniqueAttributeTypes').select('name');
    this.UniqueAttributeTypes = [];
    r.forEach((type) => {
      this.UniqueAttributeTypes.push(type.name);
    });
  }

  isUniqueType(type) {
    return this.UniqueAttributeTypes.indexOf(type) > -1;
  }

  async getMessageCount() {
    const r = await this.knex('Messages').count('* as count');
    return parseInt(r[0].count);
  }

  async getStats(id, options) {
    let sentSql = '';
    sentSql += 'SUM(CASE WHEN m.rating > (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS sent_positive, ';
    sentSql += 'SUM(CASE WHEN m.rating = (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS sent_neutral, ';
    sentSql += 'SUM(CASE WHEN m.rating < (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS sent_negative ';

    let dbTrue = true;
    let dbFalse = false;
    if (this.config.db.dialect === 'sqlite3') { // sqlite in test env fails otherwise, for some reason
      dbTrue = 1;
      dbFalse = 0;
    }

    let sent = this.knex('Messages as m')
      .innerJoin('MessageAttributes as author', (q) => {
        q.on('author.message_hash', '=', 'm.hash');
        q.on('author.is_recipient', '=', this.knex.raw('?', dbFalse));
      })
      .where('m.type', 'rating')
      .where('m.public', dbTrue);

    let receivedSql = '';
    receivedSql += 'SUM(CASE WHEN m.rating > (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS received_positive, ';
    receivedSql += 'SUM(CASE WHEN m.rating = (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS received_neutral, ';
    receivedSql += 'SUM(CASE WHEN m.rating < (m.min_rating + m.max_rating) / 2 THEN 1 ELSE 0 END) AS received_negative, ';
    receivedSql += 'MIN(m.timestamp) AS first_seen ';

    let received = this.knex('Messages as m')
      .innerJoin('MessageAttributes as recipient', (q) => {
        q.on('recipient.message_hash', '=', 'm.hash');
        q.on('recipient.is_recipient', '=', this.knex.raw('?', dbTrue));
      })
      .where('m.type', 'rating')
      .where('m.public', dbTrue);

    let identityId;
    if (options.viewpoint && options.maxDistance > -1) {
      const res = await this.knex('IdentityAttributes')
        .where({
          name: id[0],
          value: id[1],
          viewpoint_name: options.viewpoint[0],
          viewpoint_value: options.viewpoint[1],
        })
        .select('identity_id');

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

        const sentSubquery = this.knex.raw(sent).wrap('(', ') s');
        sent = this.knex('Messages as m')
          .select(this.knex.raw(sentSql))
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
          q.on('td.start_attr_name', '=', this.knex.raw('?', options.viewpoint[0]));
          q.andOn('td.start_attr_value', '=', this.knex.raw('?', options.viewpoint[1]));
          q.andOn('td.end_attr_name', '=', 'ia.name');
          q.andOn('td.end_attr_value', '=', 'ia.value');
          if (options.maxDistance > 0) {
            q.andOn('td.distance', '<=', options.maxDistance);
          }
        });

        const receivedSubquery = this.knex.raw(received).wrap('(', ') s');
        received = this.knex('Messages as m')
          .select(this.knex.raw(receivedSql))
          .innerJoin(receivedSubquery, 'm.hash', 's.hash')
          .groupBy('s.identity_id');
      }
    }
    if (!identityId) {
      sent.where({ 'author.name': id[0], 'author.value': id[1] });
      sent.groupBy('author.name', 'author.value');
      sent.select(this.knex.raw(sentSql));
      received.where({ 'recipient.name': id[0], 'recipient.value': id[1] });
      received.groupBy('recipient.name', 'recipient.value');
      received.select(this.knex.raw(receivedSql));
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
      await this.knex('IdentityStats')
        .where('identity_id', 'in', identityIds)
        .delete();
      if (identityId) {
        await this.knex('IdentityStats')
          .insert({
            identity_id: identityId,
            viewpoint_name: options.viewpoint[0],
            viewpoint_value: options.viewpoint[1],
            positive_score: res.received_positive || 0,
            negative_score: res.received_negative || 0,
          });
      }
    }

    return res;
  }

  async checkDefaultTrustList() {
    const r = await this.knex('Messages').count('* as count');
    if (parseInt(r[0].count) === 0) {
      const queries = [];
      const message = Message.createRating({
        author: [this.MY_ID],
        recipient: [['keyID', '/pbxjXjwEsojbSfdM3wGWfE24F4fX3GasmoHXY3yYPM=']],
        comment: 'Identifi seed node, trusted by default',
        rating: 10,
        context: 'identifi_network',
        public: false,
      });
      const message2 = Message.createRating({
        author: [this.MY_ID],
        recipient: [['nodeID', 'Qmbb1DRwd75rZk5TotTXJYzDSJL6BaNT1DAQ6VbKcKLhbs']],
        comment: 'Identifi IPFS seed node, trusted by default',
        rating: 10,
        context: 'identifi_network',
        public: false,
      });
      Message.sign(message, this.MY_KEY.private.pem, this.MY_KEY.public.hex);
      Message.sign(message2, this.MY_KEY.private.pem, this.MY_KEY.public.hex);
      queries.push(this.saveMessage(message));
      queries.push(this.saveMessage(message2));
      return Promise.all(queries);
    }
  }

  async ensureFreeSpace() {
    const r = await this.getMessageCount();
    if (r > this.config.maxMessageCount) {
      const nMessagesToDelete = Math.min(100, Math.ceil(this.config.maxMessageCount / 10));
      const messagesToDelete = this.knex('Messages')
        .select('hash')
        .limit(nMessagesToDelete)
        .orderBy('priority', 'asc')
        .orderBy('created', 'asc');
      await this.knex('Messages')
        .whereIn('hash', messagesToDelete).del();
      return this.knex('MessageAttributes')
        .whereIn('message_hash', messagesToDelete)
        .del();
    }
  }

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

    const distanceToSigner = await this.getTrustDistance(this.MY_ID, ['keyID', message.signerKeyHash]);
    if (distanceToSigner === -1) { // Unknown signer
      return 0;
    }
    let i;
    const queries = [];
    // Get distances to message authors
    message.signedData.author.forEach(authorId =>
      queries.push(this.getTrustDistance(this.MY_ID, authorId)));

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
  }

  static isLatest() { // param: message
    return true; // TODO: implement
  }

  async saveTrustDistance(startId, endId, distance) {
    await this.knex('TrustDistances').where({
      start_attr_name: startId[0],
      start_attr_value: startId[1],
      end_attr_name: endId[0],
      end_attr_value: endId[1],
    }).del();
    return this.knex('TrustDistances').insert({
      start_attr_name: startId[0],
      start_attr_value: startId[1],
      end_attr_name: endId[0],
      end_attr_value: endId[1],
      distance,
    });
  }

  saveIdentityAttribute(identifier, viewpoint, identityId, confirmations, refutations) {
    return this.knex('IdentityAttributes')
      .insert({
        viewpoint_name: viewpoint[0],
        viewpoint_value: viewpoint[1],
        name: identifier[0],
        value: identifier[1],
        identity_id: identityId,
        confirmations,
        refutations,
      });
  }

  getIdentityID(identifier, viewpoint) {
    return this.knex
      .from('IdentityAttributes')
      .where({
        'IdentityAttributes.viewpoint_name': viewpoint[0],
        'IdentityAttributes.viewpoint_value': viewpoint[1],
        'IdentityAttributes.name': identifier[0],
        'IdentityAttributes.value': identifier[1],
      })
      .innerJoin('UniqueAttributeTypes', 'IdentityAttributes.name', 'UniqueAttributeTypes.name')
      .select('IdentityAttributes.identity_id');
  }

  async updateIdentityIndexesByMessage(message) { // TODO: param: trustedKeyID
    if (message.signedData.type !== 'verify_identity' && message.signedData.type !== 'unverify_identity') {
      return;
    }

    const queries = [];

    // TODO: make this faster
    const viewpoints = await this.getIndexedViewpoints();
    viewpoints.forEach((vp) => {
      const viewpoint = [vp.name, vp.value];
      message.signedData.recipient.forEach((recipientId) => {
        const q = this.mapIdentityAttributes({ id: recipientId, viewpoint });
        queries.push(q);
      });
    });
    return Promise.all(queries);

    // TODO:
    // Get IndexedViewpoints as t
    // Return unless message signer and author are trusted by t
    // Find existing or new identity_id for message recipient UniqueAttributeTypes
    // If the attribute exists on the identity_id, increase confirmations or refutations
    // If the attribute doesn't exist, add it with 1 confirmation or refutation
  }

  deletePreviousMessage(message, deleteFromIpfsIndexes) {
    let i;
    let j;
    const verifyTypes = ['verify_identity', 'unverify_identity'];
    const t = message.signedData.type;
    const isVerifyMsg = verifyTypes.indexOf(t) > -1;

    const getHashesQuery = (author, recipient) => {
      const q = this.knex('Messages as m')
        .distinct('m.hash as hash', 'm.ipfs_hash as ipfs_hash', 'm.timestamp as timestamp', 'td.distance as distance')
        .innerJoin('MessageAttributes as author', (qq) => {
          qq.on('author.message_hash', '=', 'm.hash');
          qq.andOn('author.is_recipient', '=', this.knex.raw('?', false));
        })
        .innerJoin('MessageAttributes as recipient', (qq) => {
          qq.on('recipient.message_hash', '=', 'm.hash');
          qq.andOn('recipient.is_recipient', '=', this.knex.raw('?', true));
        })
        .innerJoin('UniqueAttributeTypes as ia1', 'ia1.name', 'author.name')
        .leftJoin('TrustDistances as td', (qq) => {
          qq.on('td.start_attr_name', '=', this.knex.raw('?', this.MY_ID[0]));
          qq.andOn('td.start_attr_value', '=', this.knex.raw('?', this.MY_ID[1]));
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
        q.innerJoin('UniqueAttributeTypes as ia2', 'ia2.name', 'recipient.name');
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
    };

    let hashes = [];
    let ipfsIndexKeys = [];

    const addHashes = (res) => {
      for (i = 0; i < res.length; i += 1) {
        hashes.push(res[i].hash);
        if (deleteFromIpfsIndexes && res[i].ipfs_hash && res[i].ipfs_hash.length) {
          const msg = res[i];
          ipfsIndexKeys.push(this.getMsgIndexKey(msg));
        }
      }
    };

    const getAndAddHashes = async (author, recipient) => {
      const r = await getHashesQuery(author, recipient);
      return addHashes(r);
    };

    const addInnerJoinMessageRecipient = (query, recipient, n) => {
      const as = `recipient${n}`;
      query.innerJoin(`MessageAttributes as ${as}`, (q) => {
        q.on(`${as}.message_hash`, '=', 'm.hash');
        q.on(`${as}.is_recipient`, '=', this.knex.raw('?', true));
        q.on(`${as}.name`, '=', this.knex.raw('?', recipient[0]));
        q.on(`${as}.value`, '=', this.knex.raw('?', recipient[1]));
      });
    };

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
        queries.push(this.dropMessage(hashes[i]));
      }
      ipfsIndexKeys = util.removeDuplicates(ipfsIndexKeys);
      for (i = 0; i < ipfsIndexKeys.length; i += 1) {
        console.log('deleting from index', ipfsIndexKeys[i], ipfsIndexKeys[i].substr(ipfsIndexKeys[i].indexOf(':') + 1));
        queries.push(this.ipfsMessagesByDistance.delete(ipfsIndexKeys[i]));
        queries.push(this.ipfsMessagesByTimestamp.delete(ipfsIndexKeys[i].substr(ipfsIndexKeys[i].indexOf(':') + 1)));
      }
      return Promise.all(queries);
    });
  }

  updateWotIndexesByMessage(message) {
    const queries = [];

    // TODO: remove trust distance if a previous positive rating is replaced

    const makeSubquery = (author, recipient) =>
      this.knex
        .from('IndexedViewpoints AS viewpoint')
        .innerJoin('UniqueAttributeTypes as ia', 'ia.name', this.knex.raw('?', recipient[0]))
        .innerJoin('TrustDistances AS td', (q) => {
          q.on('td.start_attr_name', '=', 'viewpoint.name')
            .andOn('td.start_attr_value', '=', 'viewpoint.value')
            .andOn('td.end_attr_name', '=', this.knex.raw('?', author[0]))
            .andOn('td.end_attr_value', '=', this.knex.raw('?', author[1]));
        })
        .leftJoin('TrustDistances AS existing', (q) => { // TODO: update existing if new distance is shorter
          q.on('existing.start_attr_name', '=', 'viewpoint.name')
            .andOn('existing.start_attr_value', '=', 'viewpoint.value')
            .andOn('existing.end_attr_name', '=', this.knex.raw('?', recipient[0]))
            .andOn('existing.end_attr_value', '=', this.knex.raw('?', recipient[1]));
        })
        .whereNull('existing.distance')
        .select(
          'viewpoint.name as start_attr_name',
          'viewpoint.value as start_attr_value',
          this.knex.raw('? as end_attr_name', recipient[0]),
          this.knex.raw('? as end_attr_value', recipient[1]),
          this.knex.raw(`${this.SQL_IFNULL}(td.distance, 0) + 1 as distance`),
        );

    const getSaveFunction = (author, recipient) =>
      (distance) => {
        if (distance > -1) {
          return this.knex('TrustDistances').insert(makeSubquery(author, recipient));
        }
      };

    if (Message.isPositive(message)) {
      let i;
      let j;
      for (i = 0; i < message.signedData.author.length; i += 1) {
        const author = message.signedData.author[i];
        const t = author[0] === 'keyID' ? author[1] : this.MY_KEY.hash; // trusted key
        for (j = 0; j < message.signedData.recipient.length; j += 1) {
          const recipient = message.signedData.recipient[j];
          const q = this.getTrustDistance(['keyID', t], ['keyID', message.signerKeyHash])
            .then(getSaveFunction(author, recipient));
          queries.push(q);
        }
      }
    }

    return Promise.all(queries);
  }

  async init(conf, ipfs) {
    if (ipfs) {
      this.ipfs = ipfs;
      this.ipfsStorage = new btree.IPFSStorage(this.ipfs);
    }
    this.config = conf;
    if (conf.db.client === 'pg') {
      this.SQL_IFNULL = 'COALESCE';
    }
    await schema.init(this.knex, this.config);
    await this.getUniqueAttributeTypes();
    await this.addTrustIndexedAttribute(this.MY_ID, MY_TRUST_INDEX_DEPTH);
    // TODO: if this.MY_ID is changed, the old one should be removed from IndexedViewpoints
    await this.mapIdentityAttributes({ id: this.MY_ID });
    await this.checkDefaultTrustList();
    if (this.ipfsStorage) { // non-blocking
      this.ipfsUtils.getIndexesFromIpfsRoot();
      if (process.env.NODE_ENV !== 'test') {
        this.ipfsUtils.saveMessagesFromIpfsIndexes();
      }
      this.ipfsUtils.keepAddingNewMessagesToIpfsIndex();
    }
  }
}

module.exports = IdentifiDB;
