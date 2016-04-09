###global describe, it, after, before ###

'use strict'
process.env.NODE_ENV = 'test'
config = require('config')
fs = require('fs')
chai = require('chai')
chaiAsPromised = require('chai-as-promised')
chai.should()
chai.use chaiAsPromised
Message = require('identifi-lib/message.js')
privKey = '-----BEGIN EC PRIVATE KEY-----\n' + 'MHQCAQEEINY+49rac3jkC+S46XN0f411svOveILjev4R3aBehwUKoAcGBSuBBAAK\n' + 'oUQDQgAEKn3lQ3+/aN6xNd9DSFrYbaPSGOzLMbb1kQZ9lCMtwc6Og4hfCMLhaSbE\n' + '3sXek8e2fvKrTp8FY1MyCL4qMeVviA==\n' + '-----END EC PRIVATE KEY-----'
pubKey = 'MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEKn3lQ3+/aN6xNd9DSFrYbaPSGOzLMbb1kQZ9lCMtwc6Og4hfCMLhaSbE3sXek8e2fvKrTp8FY1MyCL4qMeVviA=='

cleanup = ->
  fs.unlink '../' + config.db.connection.filename, (err) ->

describe 'Database', ->
  db = undefined
  hash = undefined
  before ->
    cleanup()
    # After hook fails to execute when errors are thrown
    knex = require('knex')(config.get('db'))
    db = require('../db.js')(knex)
  after ->
    cleanup()

  describe 'save and retrieve messages', ->
    it 'should initially have 0 messages', (done) ->
      db.getMessageCount().then (res) ->
        res[0].val.should.equal 0
        done()
    it 'should save a rating', (done) ->
      message = Message.createRating
        author: [['email', 'alice@example.com']]
        recipient: [['email', 'bob@example.com']]
        rating: 1

      Message.sign message, privKey, pubKey
      db.saveMessage(message).should.eventually.notify done
    it 'should save another rating', (done) ->
      message = Message.createRating
        author: [['email', 'charles@example.com']]
        recipient: [['email','bob@example.com']]
        rating: -1
      Message.sign message, privKey, pubKey
      db.saveMessage(message).should.eventually.notify done
    it 'should save yet another rating', (done) ->
      message = Message.createRating
        author: [['email', 'bob@example.com']]
        recipient: [['email', 'charles@example.com']]
        rating: 1
      Message.sign message, privKey, pubKey
      hash = message.hash
      db.saveMessage(message).should.eventually.notify done
    it 'should have 3 messages', (done) ->
      db.getMessageCount().then (res) ->
        res[0].val.should.equal 3
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
    it 'should generate a trust map', (done) ->
      db.generateTrustMap(['email', 'alice@example.com'], 3).then (res) ->
        res[0].trustmap_size.should.equal 2
        done()
    it 'should return a trust path', (done) ->
      db.getTrustPaths(['email', 'alice@example.com'], ['email', 'charles@example.com'], 3).then (res) ->
        res.length.should.equal 1
        done()
  describe 'identity search', ->
    it 'should find 4 identifiers matching "a"', (done) ->
      db.getIdentities({ searchValue: 'a' }).then (res) ->
        res.length.should.equal 4
        done()
    it 'should find 1 identifier matching "alice"', (done) ->
      db.getIdentities({ searchValue: 'alice' }).then (res) ->
        res.length.should.equal 1
        done()
    it 'should find 4 identities matching "a"', (done) ->
      db.identitySearch(['', 'a']).then (res) ->
        res.length.should.equal 4
        done()
    it 'should find 1 identity matching "alice"', (done) ->
      db.identitySearch(['', 'alice']).then (res) ->
        res.length.should.equal 1
        done()
  describe 'Priority', ->
    it 'should be 0 for a message from an unknown signer', (done) ->
      message = Message.createRating
        author: [['email', 'alice@example.com']]
        recipient: [['email', 'bob@example.com']]
        rating: 1
      Message.sign message, privKey, pubKey
      db.saveMessage(message).then ->
        db.getMessages({ where: { hash: message.hash } })
      .then (res) ->
        res[0].priority.should.equal 0
        done()
  describe 'stats', ->
    it 'should return the stats of an identifier', (done) ->
      db.getStats(['email', 'bob@example.com'], ['email', 'alice@example.com']).then (res) ->
        res.length.should.equal 1
        res[0].sentPositive.should.equal 1
        res[0].sentNeutral.should.equal 0
        res[0].sentNegative.should.equal 0
        res[0].receivedPositive.should.equal 2
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
