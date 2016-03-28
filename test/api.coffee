###global describe, it, after, before ###

'use strict'
process.env.NODE_ENV = 'test'
fs = require('fs')
request = require('request')
config = require('config')
identifi = require('identifi-lib/client')
message = require('identifi-lib/message')
chai = require('chai')
chaiAsPromised = require('chai-as-promised')
chai.should()
chai.use chaiAsPromised

privKey = '-----BEGIN EC PRIVATE KEY-----\n' + 'MHQCAQEEINY+49rac3jkC+S46XN0f411svOveILjev4R3aBehwUKoAcGBSuBBAAK\n' + 'oUQDQgAEKn3lQ3+/aN6xNd9DSFrYbaPSGOzLMbb1kQZ9lCMtwc6Og4hfCMLhaSbE\n' + '3sXek8e2fvKrTp8FY1MyCL4qMeVviA==\n' + '-----END EC PRIVATE KEY-----'

cleanup = ->
  fs.unlink './identifi_test.db', (err) ->
  fs.unlink './identifi_test.log', (err) ->

describe 'API', ->
  server = undefined
  before ->
    cleanup()
    # After hook fails to execute when errors are thrown
    server = require('../server.js')
    identifi.apiRoot = 'http://localhost:8081/api'
  after cleanup
  after ->
    console.log 'Test server at ' + config.get('port') + ' shutting down'
    server.close()
  it 'should return server status', ->
    identifi.request
      apiMethod: 'status'
  describe 'messages', ->
    describe 'create', ->
      it 'should add a new message', ->
        m = message.createRating
          author: [['email', 'alice@example.com']]
          recipient: [['email', 'bob@example.com']]
          rating: 10
        message.sign(m, privKey, 'keyID')
        identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: m
    describe 'retrieve', ->
      it 'should return message by id', ->
        identifi.request
          apiMethod: 'messages'
          apiId: '1234'
    describe 'list', ->
      it 'should list messages ordered by date'
      it 'should filter messages by type'
      it 'should filter messages by viewpoint'
      it 'should return a list of peers as identifi messages'
    describe 'delete', ->
      it 'should remove a message'
  describe 'identifiers', ->
    describe 'search', ->
      it 'should return matching identifiers / identities ordered by trust distance'
    describe 'overview', ->
      it 'should return an overview of an identifier'
    describe 'connections', ->
      it 'should return an identity, i.e. set of identifiers connected to the query param'
    describe 'connectingmsgs', ->
        it 'should messages that connect id1 and id2 to the same identity'
    describe 'sent', ->
      it 'should return messages sent by an identifier / identity'
    describe 'received', ->
      it 'should return messages received by an identifier / identity'
    describe 'getname', ->
      it 'should return a cached common name for the identifier'
    describe 'trustpaths', ->
      it 'should trustpaths from id1 to id2'
