###global describe, it, after, before ###

'use strict'
process.env.NODE_ENV = 'test'
config = require('config')
fs = require('fs')
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

describe 'Database', ->
  db = undefined
  hash = undefined
  before (done) ->
    cleanup()
    # After hook fails to execute when errors are thrown
    key = keyutil.getDefault()
    privKey = key.private.pem
    pubKey = key.public.hex
    knex = require('knex')(config.get('db'))
    db = require('../db.js')(knex)
    db.init(config).then -> done()
  after ->
    cleanup()

  describe 'save and retrieve messages', ->
    it 'should initially have 1 message', (done) ->
      db.getMessageCount().then (res) ->
        res[0].val.should.equal 1
        done()
    it 'should save a rating', (done) ->
      message = Message.createRating
        author: [['email', 'alice@example.com']]
        recipient: [['email', 'bob@example.com']]
        rating: 1
        context: 'identifi'
      Message.sign message, privKey, pubKey
      db.saveMessage(message).should.eventually.notify done
    it 'should save another rating', (done) ->
      message = Message.createRating
        author: [['email', 'charles@example.com']]
        recipient: [['email','bob@example.com']]
        rating: -1
        context: 'identifi'
      Message.sign message, privKey, pubKey
      db.saveMessage(message).should.eventually.notify done
    it 'should save yet another rating', (done) ->
      message = Message.createRating
        author: [['email', 'bob@example.com']]
        recipient: [['email', 'charles@example.com']]
        rating: 1
        context: 'identifi'
      Message.sign message, privKey, pubKey
      hash = message.hash
      db.saveMessage(message).should.eventually.notify done
    it 'should have 4 messages', (done) ->
      db.getMessageCount().then (res) ->
        res[0].val.should.equal 4
        done()
    it 'should return message by hash', (done) ->
      db.getMessages({ where: { hash: hash } }).then (res) ->
        res.length.should.equal 1
        done()
    it 'should return sent messages', (done) ->
      db.getMessages({ author: ['email', 'alice@example.com'] }).then (res) ->
        res.length.should.equal 1
        done()
    it 'should return received messages', (done) ->
      db.getMessages({ recipient: ['email', 'bob@example.com'] }).then (res) ->
        res.length.should.equal 2
        done()
    it 'should find a saved identifier', (done) ->
      db.getIdentities({ searchValue: 'bob' }).then (res) ->
        res.length.should.equal 1
        res[0].type.should.equal 'email'
        res[0].value.should.equal 'bob@example.com'
        done()
  describe 'connections', ->
    it 'should save a connection', (done) ->
      message = Message.create
        author: [['email', 'alice@example.com']]
        recipient: [['email', 'bob@example.com'], ['url', 'http://www.example.com/bob']]
        type: 'confirm_connection'
      Message.sign message, privKey, pubKey
      db.saveMessage(message).should.eventually.notify done
    it 'should save another connection', (done) ->
      message = Message.create
        author: [['email', 'alice@example.com']]
        recipient: [['email', 'bob@example.com'], ['tel', '+3581234567']]
        type: 'confirm_connection'
      Message.sign message, privKey, pubKey
      db.saveMessage(message).should.eventually.notify done
    it 'should return connecting messages', (done) ->
      db.getConnectingMessages({
        id1: ['email', 'bob@example.com']
        id2: ['url', 'http://www.example.com/bob']
      }).then (res) ->
        res.length.should.equal 1
        done()
    it 'should return connections', (done) ->
      db.getConnectedIdentifiers({
        id: ['email','bob@example.com']
        viewpoint: ['email', 'alice@example.com']
      }).then (res) ->
        res.length.should.equal 2
        done()
    it 'should return connections of type url', (done) ->
      db.getConnectedIdentifiers({
        id: ['email','bob@example.com']
        viewpoint: ['email', 'alice@example.com']
        searchedTypes: ['url']
      }).then (res) ->
        res.length.should.equal 1
        done()
  describe 'trust functions', ->
    it 'should have 1 trust indexed identifier', (done) ->
      db.getWebOfTrustIndexes().then (res) ->
        res.length.should.equal 1
        res[0].type.should.equal 'keyID'
        res[0].value.should.equal key.hash
        done()
    it 'should generate a web of trust index', (done) ->
      db.generateWebOfTrustIndex(['email', 'alice@example.com'], 3, true, key.hash).then (res) ->
        res[0].wot_size.should.equal 2
        done()
    it 'should have 2 trust indexed identifiers', (done) ->
      db.getWebOfTrustIndexes().then (res) ->
        res.length.should.equal 2
        res[1].type.should.equal 'email'
        res[1].value.should.equal 'alice@example.com'
        done()
    it 'should return a trust path', (done) ->
      db.getTrustPaths(['email', 'alice@example.com'], ['email', 'charles@example.com'], 3).then (res) ->
        res.length.should.equal 1
        done()
    it 'should not extend trust with a msg from an untrusted signer', (done) ->
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
        res[0].wot_size.should.equal 2
        done()
      ###
      db.getTrustPaths(['email', 'alice@example.com'], ['email', 'charles@example.com'], 3).then (res) ->
        res.length.should.equal 1
        done() ###
  describe 'identity search', ->
    it 'should find 7 identifiers matching "a"', (done) ->
      db.getIdentities({ searchValue: 'a' }).then (res) ->
        res.length.should.equal 7
        done()
    it 'should find 1 identifier matching "alice"', (done) ->
      db.getIdentities({ searchValue: 'alice' }).then (res) ->
        res.length.should.equal 1
        done()
    it 'should find 7 identities matching "a"', (done) ->
      db.identitySearch(['', 'a']).then (res) ->
        res.length.should.equal 7
        done()
    it 'should find 1 identity matching "alice"', (done) ->
      db.identitySearch(['', 'alice']).then (res) ->
        res.length.should.equal 1
        done()
  describe 'Priority', ->
    it 'should be 100 for a message signed & authored by own key, recipient type keyID', (done) ->
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
        done()
    it 'should be 98 for a message signed by own key', (done) ->
      message = Message.createRating
        author: [['email', 'alice@example.com']]
        recipient: [['email', 'bob@example.com']]
        rating: 1
        context: 'identifi'
      Message.sign message, privKey, pubKey
      db.saveMessage(message).then ->
        db.getMessages({ where: { hash: message.hash } })
      .then (res) ->
        res[0].priority.should.equal 98
        done()
    it 'should be 0 for a message from an unknown signer', (done) ->
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
        done()
    it 'should be 48 for a message from a 1st degree trusted signer', (done) ->
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
          res[0].priority.should.equal 48
          done()
    it 'should be 31 for a message from a 2nd degree trusted signer', (done) ->
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
          res[0].priority.should.equal 31
          done()
  describe 'stats', ->
    it 'should return the stats of an identifier', (done) ->
      db.getStats(['email', 'bob@example.com'], ['email', 'alice@example.com']).then (res) ->
        res.length.should.equal 1
        res[0].sentPositive.should.equal 1
        res[0].sentNeutral.should.equal 0
        res[0].sentNegative.should.equal 0
        res[0].receivedPositive.should.equal 3
        res[0].receivedNeutral.should.equal 0
        res[0].receivedNegative.should.equal 1
        res[0].firstSeen.should.not.be.empty
        done()
  describe 'delete', ->
    it 'should delete a message', (done) ->
      originalCount = null
      db.getMessageCount().then (res) ->
        originalCount = res[0].val
        db.dropMessage(hash)
      .then (res) ->
        res.should.be.true
        db.getMessageCount()
      .then (res) ->
        (originalCount - res[0].val).should.equal 1
        done()
  describe 'maxMessageCount', ->
    lowPrioMsg = null
    before (done) ->
      # Save a 0-priority message
      k = keyutil.generate()
      lowPrioMsg = Message.createRating
        author: [['email', 'user1@example.com']]
        recipient: [['email', 'user2@example.com']]
        rating: 1
        context: 'identifi'
      Message.sign lowPrioMsg, k.private.pem, k.public.hex
      db.saveMessage(lowPrioMsg).then -> done()

    it 'should not be exceeded', (done) ->
      saveMessages = (counter, max) ->
        message = Message.createRating
          author: [['email', 'user1@example.com']]
          recipient: [['email', 'user2@example.com']]
          rating: 1
          context: 'identifi'
        Message.sign message, privKey, pubKey
        r = db.saveMessage(message)
        if counter <= max
          r.then -> saveMessages(counter + 1, max)
        else
          return r

      saveMessages(0, 120).then ->
        db.getMessageCount()
      .then (res) ->
        res[0].val.should.be.below 100
        done()

    it 'should have deleted the 0-priority message', (done) ->
      db.getMessages({ where: { hash: lowPrioMsg.hash } }).then (res) ->
        res.length.should.equal 0
        done()
