###global describe, it, after, before ###

'use strict'
process.env.NODE_ENV = 'test'
config = require('config')
fs = require('fs')
chai = require('chai')
chaiAsPromised = require('chai-as-promised')
chai.should()
chai.use chaiAsPromised
Message = require('../message.js')
privKey = '-----BEGIN EC PRIVATE KEY-----\n' + 'MHQCAQEEINY+49rac3jkC+S46XN0f411svOveILjev4R3aBehwUKoAcGBSuBBAAK\n' + 'oUQDQgAEKn3lQ3+/aN6xNd9DSFrYbaPSGOzLMbb1kQZ9lCMtwc6Og4hfCMLhaSbE\n' + '3sXek8e2fvKrTp8FY1MyCL4qMeVviA==\n' + '-----END EC PRIVATE KEY-----'
pubKey = 'MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEKn3lQ3+/aN6xNd9DSFrYbaPSGOzLMbb1kQZ9lCMtwc6Og4hfCMLhaSbE3sXek8e2fvKrTp8FY1MyCL4qMeVviA=='

cleanup = ->
  fs.unlink('./identifi_test.db', (err) -> {});

describe 'Database', ->
  db = undefined
  before ->
    cleanup()
    # After hook fails to execute when errors are thrown
    knex = require('knex')(config.get('db'))
    db = require('../db.js')(knex)
  after cleanup
  it 'should initially have 0 messages', (done) ->
    db.getMessageCount().then (res) ->
      res[0].val.should.equal 0
      done()
  it 'should save a rating', (done) ->
    message = Message.createRating([ [
      'email'
      'alice@example.com'
    ] ], [ [
      'email'
      'bob@example.com'
    ] ], 1)
    Message.sign message, privKey, pubKey
    db.saveMessage(message).should.eventually.notify done
  it 'should save another rating', (done) ->
    message = Message.createRating([ [
      'email'
      'charles@example.com'
    ] ], [ [
      'email'
      'bob@example.com'
    ] ], -1)
    Message.sign message, privKey, pubKey
    db.saveMessage(message).should.eventually.notify done
  hash = undefined
  it 'should save yet another rating', (done) ->
    message = Message.createRating([ [
      'email'
      'bob@example.com'
    ] ], [ [
      'email'
      'charles@example.com'
    ] ], 1)
    Message.sign message, privKey, pubKey
    hash = message.hash
    db.saveMessage(message).should.eventually.notify done
  it 'should have 3 messages', (done) ->
    db.getMessageCount().then (res) ->
      res[0].val.should.equal 3
      done()
  it 'should return message by hash', (done) ->
    db.getMessage(hash).then (res) ->
      res.length.should.equal 1
      done()
  it 'should return sent messages', (done) ->
    db.getSent([
      'email'
      'alice@example.com'
    ]).then (res) ->
      res.length.should.equal 1
      done()
  it 'should return received messages', (done) ->
    db.getReceived([
      'email'
      'bob@example.com'
    ]).then (res) ->
      res.length.should.equal 2
      done()
  it 'should find a saved identifier', (done) ->
    db.identifierSearch('bob').then (res) ->
      res.length.should.equal 1
      res[0].type.should.equal 'email'
      res[0].value.should.equal 'bob@example.com'
      done()
  it 'should save a connection', (done) ->
    message = Message.createConfirmConnection([ [
      'email'
      'alice@example.com'
    ] ], [
      [
        'email'
        'bob@example.com'
      ]
      [
        'url'
        'http://www.example.com/bob'
      ]
    ])
    Message.sign message, privKey, pubKey
    db.saveMessage(message).should.eventually.notify done
  it 'should return connecting messages', (done) ->
    db.getConnectingMessages([
      'email'
      'bob@example.com'
    ], [
      'url'
      'http://www.example.com/bob'
    ]).then (res) ->
      res.length.should.equal 1
      done()
  it 'should return connections', (done) ->
    db.getConnectedIdentifiers([
      'email'
      'bob@example.com'
    ], [], 10, 0, [
      'email'
      'alice@example.com'
    ]).then (res) ->
      res.length.should.equal 1
      done()
  it 'should generate a trust map', (done) ->
    db.generateTrustMap([
      'email'
      'alice@example.com'
    ], 3).then (res) ->
      res[0].val.should.equal 2
      done()
  it 'should return a trust path', (done) ->
    db.getTrustPaths([
      'email'
      'alice@example.com'
    ], [
      'email'
      'charles@example.com'
    ], 3).then (res) ->
      res.length.should.equal 1
      done()
  it 'should find 4 identifiers matching "a"', (done) ->
    db.identifierSearch('a').then (res) ->
      res.length.should.equal 4
      done()
  it 'should find 1 identifier matching "alice"', (done) ->
    db.identifierSearch('alice').then (res) ->
      res.length.should.equal 1
      done()
  it 'should find 4 identities matching "a"', (done) ->
    db.identitySearch([
      ''
      'a'
    ]).then (res) ->
      res.length.should.equal 4
      done()
  it 'should find 1 identity matching "alice"', (done) ->
    db.identitySearch([
      ''
      'alice'
    ]).then (res) ->
      res.length.should.equal 1
      done()
  it 'should initially have no private keys', (done) ->
    db.listMyKeys().then (res) ->
      res.length.should.equal 0
      done()
  it 'should import a private key', (done) ->
    db.importPrivateKey(privKey).then(->
      db.listMyKeys()
    ).then (res) ->
      res.length.should.equal 1
      done()
  describe 'Priority', ->
    it 'should be 0 for a message from an unknown signer', (done) ->
      message = Message.createRating([ [
        'email'
        'alice@example.com'
      ] ], [ [
        'email'
        'bob@example.com'
      ] ], 1)
      Message.sign message, privKey, pubKey
      db.saveMessage(message).then(->
        db.getMessage message.hash
      ).then (res) ->
        res[0].priority.should.equal 0
        done()
  it 'should return an overview of an identifier', (done) ->
    db.overview([
      'email'
      'bob@example.com'
    ], [
      'email'
      'alice@example.com'
    ]).then (res) ->
      res.length.should.equal 1
      res[0].sentPositive.should.equal 1
      res[0].sentNeutral.should.equal 0
      res[0].sentNegative.should.equal 0
      res[0].receivedPositive.should.equal 1
      res[0].receivedNeutral.should.equal 0
      res[0].receivedNegative.should.equal 1
      res[0].firstSeen.should.be.above 0
      done()
  it 'should delete a message', (done) ->
    db.dropMessage(hash).then((res) ->
      res.should.be.true
      db.getMessageCount()
    ).then (res) ->
      res[0].val.should.equal 3
      done()
