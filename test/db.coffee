###global describe, it, after, before ###

'use strict'
process.env.NODE_ENV = 'test'
config = require('config')
fs = require('fs')
P = require('bluebird')
chai = require('chai')
chaiAsPromised = require('chai-as-promised')
chai.should()
chai.use chaiAsPromised
Message = require('identifi-lib/message')
keyutil = require('identifi-lib/keyutil')
key = null
anotherKey = null
yetAnotherKey = null
privKey = null
pubKey = null

cleanup = ->
  fs.unlink '../' + config.db.connection.filename, (err) ->

resetPostgres = (knex) ->
  if config.db.client == 'pg'
    return knex.raw('drop schema public cascade')
      .then -> knex.raw('create schema public')
  else
    return new P (resolve) -> resolve()

describe 'Database', ->
  db = undefined
  hash = undefined
  before ->
    cleanup()
    # After hook fails to execute when errors are thrown
    key = keyutil.getDefault()
    privKey = key.private.pem
    pubKey = key.public.hex
    knex = require('knex')(config.get('db'))
    resetPostgres(knex)
    .then ->
      db = require('../db.js')(knex)
      db.init(config)
  after ->
    cleanup()

  describe 'save and retrieve messages', ->
    it 'should initially have 1 message', ->
      db.getMessageCount().then (res) ->
        res.should.equal 1
    it 'should save a rating', ->
      message = Message.createRating
        author: [['email', 'alice@example.com']]
        recipient: [['email', 'bob@example.com']]
        rating: 1
        context: 'identifi'
      Message.sign message, privKey, pubKey
      db.saveMessage(message)
    it 'should save another rating', ->
      message = Message.createRating
        author: [['email', 'charles@example.com']]
        recipient: [['email','bob@example.com']]
        rating: -1
        context: 'identifi'
      Message.sign message, privKey, pubKey
      db.saveMessage(message)
    it 'should save yet another rating', ->
      message = Message.createRating
        author: [['email', 'bob@example.com']]
        recipient: [['email', 'charles@example.com']]
        rating: 1
        context: 'identifi'
      Message.sign message, privKey, pubKey
      db.saveMessage(message).then (res) ->
        hash = res.hash
    it 'should have 4 messages', ->
      db.getMessageCount().then (res) ->
        res.should.equal 4
    it 'should return message by hash', ->
      db.getMessages({ where: { hash: hash } }).then (res) ->
        res.length.should.equal 1
    it 'should return sent messages', ->
      db.getMessages({ author: ['email', 'alice@example.com'] }).then (res) ->
        res.length.should.equal 1
    it 'should return received messages', ->
      db.getMessages({ recipient: ['email', 'bob@example.com'] }).then (res) ->
        res.length.should.equal 2
    ### it 'should find a saved attribute', ->
      db.getIdentityAttributes({ searchValue: 'bob' }).then (res) ->
        console.log(res)
        res.length.should.equal 1
        res[0].name.should.equal 'email'
        res[0].value.should.equal 'bob@example.com'
        done() ###
  describe 'verifications', ->
    it 'should save a connection', ->
      message = Message.create
        author: [['email', 'alice@example.com']]
        recipient: [['email', 'bob@example.com'], ['url', 'http://www.example.com/bob']]
        type: 'verify_identity'
      Message.sign message, privKey, pubKey
      db.saveMessage(message)
    it 'should save another connection', ->
      message = Message.create
        author: [['email', 'alice@example.com']]
        recipient: [['email', 'bob@example.com'], ['tel', '+3581234567']]
        type: 'verify_identity'
      Message.sign message, privKey, pubKey
      db.saveMessage(message)
    it 'should return connecting messages', ->
      db.getConnectingMessages({
        attr1: ['email', 'bob@example.com']
        attr2: ['url', 'http://www.example.com/bob']
      }).then (res) ->
        res.length.should.equal 1
    ### it 'should return connections', ->
      db.mapIdentityAttributes({
        id: ['email','bob@example.com']
        viewpoint: ['email', 'alice@example.com']
      }).then (res) ->
        res.length.should.equal 2
    it 'should return connections of attribute url', ->
      db.mapIdentityAttributes({
        id: ['email','bob@example.com']
        viewpoint: ['email', 'alice@example.com']
        searchedAttributes: ['url']
      }).then (res) ->
        res.length.should.equal 1
        ###
  describe 'trust functions', ->
    it 'should have 1 trust indexed attribute', ->
      db.getTrustIndexedAttributes().then (res) ->
        res.length.should.equal 1
        res[0].name.should.equal 'keyID'
        res[0].value.should.equal key.hash
    it 'should generate a web of trust index', ->
      db.generateWebOfTrustIndex(['email', 'alice@example.com'], 3, true, key.hash).then (res) ->
        res.should.equal 3
    it 'should have 2 trust indexed attributes', ->
      db.getTrustIndexedAttributes().then (res) ->
        res.length.should.equal 2
        res[1].name.should.equal 'email'
        res[1].value.should.equal 'alice@example.com'
    it 'should not extend trust with a msg from an untrusted signer', ->
      untrustedKey = keyutil.generate()
      message = Message.createRating
        author: [['email', 'alice@example.com']]
        recipient: [['email', 'darwin@example.com']]
        rating: 1
        context: 'identifi'
      Message.sign message, untrustedKey.private.pem, untrustedKey.public.hex
      db.saveMessage(message).then ->
        db.generateWebOfTrustIndex(['email', 'alice@example.com'], 3, true, key.hash)
      .then (res) ->
        res.should.equal 3
      ###
      db.getTrustPaths(['email', 'alice@example.com'], ['email', 'charles@example.com'], 3).then (res) ->
        res.length.should.equal 1
        ###
    it 'should extend trust on rating message save', ->
      message = Message.create
        author: [['email', 'alice@example.com']]
        recipient: [['email', 'fennie@example.com']]
        rating: 1
        maxRating: 10
        minRating: -10
        context: 'identifi'
        type: 'rating'
      Message.sign message, privKey, pubKey
      db.saveMessage(message).then ->
        db.getTrustDistance(['email', 'alice@example.com'], ['email', 'fennie@example.com']).then (res) ->
          res.should.equal 1
  describe 'identity search', ->
    ### it 'should find 7 attributes matching "a"', ->
      db.getIdentityAttributes({ searchValue: 'a' }).then (res) ->
        res.length.should.equal 7
    it 'should find 1 attribute matching "alice"', ->
      db.getIdentityAttributes({ searchValue: 'alice' }).then (res) ->
        res.length.should.equal 1
        ###
  describe 'Priority', ->
    it 'should be 100 for a message signed & authored by own key, recipient attribute keyID', ->
      anotherKey = keyutil.generate()
      message = Message.createRating
        author: [['keyID', key.hash]]
        recipient: [['keyID', anotherKey.hash]]
        rating: 10
        context: 'identifi'
      Message.sign message, privKey, pubKey
      db.saveMessage(message).then ->
        db.getMessages({ where: { hash: message.hash } })
      .then (res) ->
        res[0].priority.should.equal 100
    it 'should be 99 for a message signed & authored by own key, recipient attribute email', ->
      message = Message.createRating
        author: [['keyID', key.hash]]
        recipient: [['email', 'alice@example.com']]
        rating: 10
        context: 'identifi'
      Message.sign message, privKey, pubKey
      db.saveMessage(message).then ->
        db.getMessages({ where: { hash: message.hash } })
      .then (res) ->
        res[0].priority.should.equal 99
    it 'should be 81 for a message signed by own key, authored by known', ->
      message = Message.createRating
        author: [['email', 'alice@example.com']]
        recipient: [['email', 'bob@example.com']]
        rating: 1
        context: 'identifi'
      Message.sign message, privKey, pubKey
      db.saveMessage(message).then ->
        db.getMessages({ where: { hash: message.hash } })
      .then (res) ->
        res[0].priority.should.equal 81
    it 'should be 48 for a message signed by own key, authored by unknown', ->
      message = Message.createRating
        author: [['email', 'user@example.com']]
        recipient: [['email', 'bob@example.com']]
        rating: 1
        context: 'identifi'
      Message.sign message, privKey, pubKey
      db.saveMessage(message).then ->
        db.getMessages({ where: { hash: message.hash } })
      .then (res) ->
        res[0].priority.should.equal 48
    it 'should be 0 for a message from an unknown signer', ->
      message = Message.createRating
        author: [['email', 'alice@example.com']]
        recipient: [['email', 'bob@example.com']]
        rating: 1
        context: 'identifi'
      unknownKey = keyutil.generate()
      Message.sign message, unknownKey.private.pem, unknownKey.public.hex
      db.saveMessage(message).then ->
        db.getMessages({ where: { hash: message.hash } })
      .then (res) ->
        res[0].priority.should.equal 0
    it 'should be 23 for a message from a 1st degree trusted signer, unknown author', ->
      message = Message.createRating
        author: [['email', 'user1@example.com']]
        recipient: [['email', 'user2@example.com']]
        rating: 1
        context: 'identifi'
      Message.sign message, anotherKey.private.pem, anotherKey.public.hex
      db.saveMessage(message)
      .then ->
        db.getMessages({ where: { hash: message.hash } })
        .then (res) ->
          res[0].priority.should.equal 23
    it 'should be 40 for a message from a 1st degree trusted signer, known author', ->
      message = Message.createRating
        author: [['email', 'alice@example.com']]
        recipient: [['email', 'user2@example.com']]
        rating: 1
        context: 'identifi'
      Message.sign message, anotherKey.private.pem, anotherKey.public.hex
      db.saveMessage(message)
      .then ->
        db.getMessages({ where: { hash: message.hash } })
        .then (res) ->
          res[0].priority.should.equal 40
    it 'should be 15 for a message from a 2nd degree trusted signer, unknown author', ->
      yetAnotherKey = keyutil.generate()
      message = Message.createRating
        author: [['keyID', anotherKey.hash]]
        recipient: [['keyID', yetAnotherKey.hash]]
        rating: 10
        context: 'identifi'
      Message.sign message, anotherKey.private.pem, anotherKey.public.hex
      db.saveMessage(message)
      .then ->
        message = Message.createRating
          author: [['email', 'user1@example.com']]
          recipient: [['email', 'user2@example.com']]
          rating: 1
          context: 'identifi'
        Message.sign message, yetAnotherKey.private.pem, yetAnotherKey.public.hex
        db.saveMessage(message)
      .then ->
        db.getMessages({ where: { hash: message.hash } })
        .then (res) ->
          res[0].priority.should.equal 15
  describe 'stats', ->
    it 'should return the stats of an attribute', ->
      db.getStats(['email', 'bob@example.com'], { viewpoint: ['email', 'alice@example.com']}).then (res) ->
        res.sent_positive.should.equal 1
        res.sent_neutral.should.equal 0
        res.sent_negative.should.equal 0
        res.received_positive.should.equal 3
        res.received_neutral.should.equal 0
        res.received_negative.should.equal 1
        res.first_seen.should.not.be.empty
  describe 'delete', ->
    it 'should delete a message', ->
      originalCount = null
      db.getMessageCount().then (res) ->
        originalCount = res
        db.dropMessage(hash)
      .then (res) ->
        res.should.be.true
        db.getMessageCount()
      .then (res) ->
        (originalCount - res).should.equal 1
  describe 'maxMessageCount', ->
    lowPrioMsg = null
    before ->
      # Save a 0-priority message
      k = keyutil.generate()
      lowPrioMsg = Message.createRating
        author: [['email', 'user1@example.com']]
        recipient: [['email', 'user2@example.com']]
        rating: 1
        context: 'identifi'
      Message.sign lowPrioMsg, k.private.pem, k.public.hex
      db.saveMessage(lowPrioMsg)

    it 'should not be exceeded', ->
      @timeout 10000
      saveMessages = (counter, max) ->
        message = Message.createRating
          author: [['email', 'user1@example.com']]
          recipient: [['email', counter + 'user2@example.com']]
          rating: 1
          context: 'identifi'
        Message.sign message, privKey, pubKey
        r = db.saveMessage(message)
        if counter <= max
          r.then -> saveMessages(counter + 1, max)
        else
          return r

      saveMessages(0, config.maxMessageCount + 20).then ->
        db.getMessageCount()
      .then (res) ->
        res.should.be.below config.maxMessageCount

    it 'should have deleted the 0-priority message', ->
      db.getMessages({ where: { hash: lowPrioMsg.hash } }).then (res) ->
        res.length.should.equal 0
