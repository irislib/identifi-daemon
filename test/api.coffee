###global describe, it, after, before ###

'use strict'
process.env.NODE_ENV = 'test'
fs = require('fs')
P = require('bluebird')
osHomedir = require('os-homedir')
errors = require('request-promise/errors')
config = require('config')
identifi = require('identifi-lib/client')
message = require('identifi-lib/message')
keyutil = require('identifi-lib/keyutil')
chai = require('chai')
chaiAsPromised = require('chai-as-promised')
chai.should()
chai.use chaiAsPromised

datadir = process.env.IDENTIFI_DATADIR || (osHomedir() + '/.identifi')
myKey = null
privKeyPEM = null
hex = null
m = null

cleanup = ->
  fs.unlink './identifi_test.db', (err) ->
  fs.unlink './identifi_test.log', (err) ->

resetPostgres = ->
  if config.db.client == 'pg'
    knex = require('knex')(config.get('db'))
    return knex.raw('drop schema public cascade')
      .then -> knex.raw('create schema public')
  else
    return new P (resolve) -> resolve()

describe 'API', ->
  server = undefined
  socket = undefined
  before (done) ->
    cleanup()
    resetPostgres().then ->
      # After hook fails to execute when errors are thrown
      server = require('../server.js')

      myKey = keyutil.getDefault(datadir)
      privKeyPEM = myKey.private.pem
      hex = myKey.public.hex

      identifi.apiRoot = 'http://127.0.0.1:4944/api'
      server.ready
    .then ->
      socket = identifi.getSocket({ isPeer: true })
      done()
  after cleanup
  after ->
    console.log 'Test server at ' + config.get('port') + ' shutting down'
    server.close()
  it 'should return server status', ->
    identifi.request({})
  describe 'messages', ->
    describe 'create', ->
      it 'should add rating 10 from alice@example.com to bob@example.com', ->
        m = message.createRating
          author: [['email', 'alice@example.com']]
          recipient: [['email', 'bob@example.com']]
          rating: 10
          context: 'identifi'
        message.sign(m, privKeyPEM, hex)
        r = identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: m
      it 'should add rating 10 from bob@example.com to charles@example.com', ->
        m = message.createRating
          author: [['email', 'bob@example.com']]
          recipient: [['email', 'charles@example.com']]
          rating: 10
          context: 'identifi'
        message.sign(m, privKeyPEM, hex)
        r = identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: m
      it 'should add rating 10 from charles@example.com to david@example.com', ->
        m = message.createRating
          author: [['email', 'charles@example.com']]
          recipient: [['email', 'david@example.com']]
          rating: 10
          context: 'identifi'
        message.sign(m, privKeyPEM, hex)
        r = identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: m
      it 'should add rating -1 from charles@example.com to bob@example.com', ->
        m = message.createRating
          author: [['email', 'charles@example.com']]
          recipient: [['email', 'bob@example.com']]
          rating: -1
          context: 'identifi'
        message.sign(m, privKeyPEM, hex)
        r = identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: m
      it 'should add rating -10 from nobody@example.com to bob@example.com', ->
        m = message.createRating
          author: [['email', 'nobody@example.com']]
          recipient: [['email', 'bob@example.com']]
          rating: -10
          context: 'identifi'
        message.sign(m, privKeyPEM, hex)
        r = identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: m
      it 'add a verification msg', ->
        m = message.create
          author: [['email', 'alice@example.com']]
          recipient: [['email', 'bob@example.com'], ['name', 'Bob the Builder']]
          type: 'verify_identity'
        message.sign(m, privKeyPEM, hex)
        r = identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: m
      it 'add another verification msg', (done) ->
        m = message.create
          author: [['email', 'bob@example.com']]
          recipient: [['email', 'charles@example.com'], ['url', 'http://twitter.com/charles']]
          type: 'verify_identity'
        message.sign(m, privKeyPEM, hex)
        r = identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: m
        r.then -> done()
      it 'should add an unsigned and authorless message when authorization is provided', (done) ->
        msg =
          recipient: [['email', 'charles@example.com'], ['url', 'http://twitter.com/charles']]
          type: 'verify_identity'
        r = identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: msg
          headers:
            'Authorization': 'Bearer ' + identifi.getJwt(privKeyPEM, { user: { idType: 'email', idValue: 'bob@example.com', name: 'Bob' } })
        r.then (res) ->
          m = res
          done()
    describe 'retrieve', ->
      it 'should fail if the message was not found', ->
        r = identifi.request
              apiMethod: 'messages'
              apiId: '1234'
        r.should.be.rejected
      it 'should return the previously saved message', (done) ->
        r = identifi.request
          apiMethod: 'messages'
          apiId: m.hash
        r.then (res) ->
          res.hash.should.equal m.hash
          done()
    describe 'non-public messages', ->
      privMsg = null
      it 'should add a non-public msg', (done) ->
        privMsg = message.create
          author: [['email', 'bob@example.com']]
          recipient: [['email', 'charles@example.com'], ['url', 'http://twitter.com/charles']]
          type: 'verify_identity'
          public: false
        message.sign(privMsg, privKeyPEM, hex)
        identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: privMsg
        .then -> done()
      it 'should not be visible in message listing', ->
        r = identifi.request
          apiMethod: 'messages'
          apiId: privMsg.hash
        r.should.be.rejected
      it 'should not be returned by hash', (done) ->
        identifi.request
          apiMethod: 'messages'
        .then (res) ->
          res[0].hash.should.not.equal privMsg.hash
          done()
    describe 'wot', ->
      it 'should not generate a web of trust without auth', ->
        r = identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'alice@example.com'
          apiAction: 'generatewotindex'
          qs:
            depth: 3
        r.should.be.rejected
      it 'should generate a web of trust index', (done) ->
        r = identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'alice@example.com'
          apiAction: 'generatewotindex'
          qs:
            depth: 3
          headers:
            'Authorization': 'Bearer ' + identifi.getJwt(privKeyPEM, { admin: true })
        r.then (res) ->
          res.should.equal 4
          done()
    describe 'list', ->
      it 'should list messages ordered by date', (done) ->
        r = identifi.request
          apiMethod: 'messages'
        r.then (res) ->
          res[0].hash.should.equal m.hash
          done()
      it 'should filter messages by type', (done) ->
        r = identifi.request
          apiMethod: 'messages'
          qs:
            type: 'rating'
        r.then (res) ->
          msg.type.should.equal 'rating' for msg in res
          done()
      it 'should filter messages by rating type positive', (done) ->
        r = identifi.request
          apiMethod: 'messages'
          qs:
            type: 'rating:positive'
        r.then (res) ->
          msg.type.should.equal 'rating' for msg in res
          msg.rating.should.be.above (msg.max_rating + msg.min_rating) / 2
          done()
      it 'should filter messages by rating type negative', (done) ->
        r = identifi.request
          apiMethod: 'messages'
          qs:
            type: 'rating:negative'
        r.then (res) ->
          msg.type.should.equal 'rating' for msg in res
          msg.rating.should.be.below (msg.max_rating + msg.min_rating) / 2
          done()
      it 'should filter messages by viewpoint, max_distance 1', (done) ->
        r = identifi.request
          apiMethod: 'messages'
          qs:
            viewpoint_name: 'email'
            viewpoint_value: 'alice@example.com'
            max_distance: 1
        r.then (res) ->
          res.length.should.equal 7
          done()
      it 'should filter messages by timestamp_lte', (done) ->
        r = identifi.request
          apiMethod: 'messages'
          qs:
            timestamp_lte: m.signedData.timestamp
        r.then (res) ->
          res.length.should.equal 8
          done()
      it 'should filter messages by timestamp_gte', (done) ->
        r = identifi.request
          apiMethod: 'messages'
          qs:
            timestamp_gte: m.signedData.timestamp
        r.then (res) ->
          res.length.should.equal 1
          done()
      it 'should filter messages by viewpoint, max_distance 2', (done) ->
        r = identifi.request
          apiMethod: 'messages'
          qs:
            viewpoint_name: 'email'
            viewpoint_value: 'alice@example.com'
            max_distance: 2
        r.then (res) ->
          res.length.should.equal 7
          done()
    describe 'delete', ->
      it 'should fail without auth', ->
        r = identifi.request
          apiMethod: 'messages'
          apiId: m.hash
          method: 'DELETE'
        r.should.be.rejected
      it 'should delete the previously saved message', (done) ->
        r = identifi.request
          apiMethod: 'messages'
          apiId: m.hash
          method: 'DELETE'
          headers:
            'Authorization': 'Bearer ' + identifi.getJwt(privKeyPEM, { admin: true })
        r.then (res) ->
          res.should.equal 'OK'
          done()
      it 'should fail if the message was not found', ->
        r = identifi.request
          apiMethod: 'messages'
          apiId: '1234'
          method: 'DELETE'
          headers:
            'Authorization': 'Bearer ' + identifi.getJwt(privKeyPEM, { admin: true })
        r.should.be.rejected
      it 'should have removed the message', ->
        r = identifi.request
              apiMethod: 'messages'
              apiId: m.hash
        r.should.be.rejected
  describe 'identities', ->
      it 'should return an empty set if an identity was not found', (done) ->
        r = identifi.request
              apiMethod: 'identities'
              apiId: 'bob@example.com'
              apiIdType: 'nope'
        r.then (res) ->
          res.should.be.empty
          done()
    describe 'retrieve', ->
      it 'should return an identity', (done) ->
        r = identifi.request
          apiMethod: 'identities'
          apiId: 'bob@example.com'
          apiIdType: 'email'
        r.then (res) ->
          res.should.not.be.empty
          done()
    describe 'list', ->
      ### it 'should return identities', (done) ->
        r = identifi.request
          apiMethod: 'identities'
          qs:
            viewpoint_name: 'email'
            viewpoint_value: 'alice@example.com'
        r.then (res) ->
          console.log(res)
          res.length.should.equal 5
          done() ###
      it 'should filter identities by attribute name', (done) ->
        r = identifi.request
          apiMethod: 'identities'
          qs:
            attr_name: 'email'
        r.then (res) ->
          res.length.should.equal 2
          done()
      it 'should filter by search query', (done) -> # TODO: fix?
        r = identifi.request
          apiMethod: 'identities'
          qs:
            search_value: 'i'
        r.then (res) ->
          #res.length.should.equal 5
          #res[0].value.should.equal 'Bob the Builder'
          #res[1].value.should.equal 'alice@example.com'
          #res[2].value.should.equal 'david@example.com'
          #res[3].value.should.equal 'http://twitter.com/charles'
          done()
      it 'should return a list of peers as identifi identities', (done) ->
        r = identifi.request
          apiMethod: 'identities'
          qs:
            attr_name: 'identifi_node'
        r.then (res) ->
          res.length.should.equal 0
          done()
    describe 'verifications', ->
      it 'should return an identity, i.e. set of attributes connected to the query param', (done) ->
        r = identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'bob@example.com'
          apiAction: 'verifications'
        r.then (res) ->
          res.should.not.be.empty
          done()
    describe 'connecting_msgs', ->
        it 'should return messages that connect id1 and id2 to the same identity', (done) ->
          r = identifi.request
            apiMethod: 'identities'
            apiIdType: 'email'
            apiId: 'bob@example.com'
            apiAction: 'connecting_msgs'
            qs:
              target_name: 'name'
              target_value: 'Bob the Builder'
          r.then (res) ->
            res.should.not.be.empty
            done()
    describe 'stats', ->
      it 'should return the stats of an attribute, no viewpoint', (done) ->
        r = identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'bob@example.com'
          apiAction: 'stats'
        r.then (res) ->
          res.length.should.equal 1
          res[0].sent_positive.should.equal 1
          res[0].sent_neutral.should.equal 0
          res[0].sent_negative.should.equal 0
          res[0].received_positive.should.equal 1
          res[0].received_neutral.should.equal 0
          res[0].received_negative.should.equal 2
          res[0].first_seen.should.not.be.empty
          done()
      it 'should return the stats of an attribute, using a viewpoint & max_distance 1', (done) ->
        r = identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'bob@example.com'
          apiAction: 'stats'
          qs:
            viewpoint_name: 'email'
            viewpoint_value: 'alice@example.com'
            max_distance: 1
        r.then (res) ->
          return done() # TODO: temporarily disabled
          res.length.should.equal 1
          res[0].sent_positive.should.equal 1
          res[0].sent_neutral.should.equal 0
          res[0].sent_negative.should.equal 0
          res[0].received_positive.should.equal 1
          res[0].received_neutral.should.equal 0
          res[0].received_negative.should.equal 0
          res[0].first_seen.should.not.be.empty
          done()
        .catch (e) ->
          done(e)
      it 'should return the stats of an attribute, using a viewpoint & max_distance 2', (done) ->
        r = identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'bob@example.com'
          apiAction: 'stats'
          qs:
            viewpoint_name: 'email'
            viewpoint_value: 'alice@example.com'
            max_distance: 1
        r.then (res) ->
          return done() # TODO: temporarily disabled
          res.length.should.equal 1
          res[0].sent_positive.should.equal 1
          res[0].sent_neutral.should.equal 0
          res[0].sent_negative.should.equal 0
          res[0].received_positive.should.equal 1
          res[0].received_neutral.should.equal 0
          res[0].received_negative.should.equal 1
          res[0].first_seen.should.not.be.empty
          done()
        .catch (e) ->
          done(e)
    describe 'sent', ->
      it 'should return messages sent by an attribute / identity', (done) ->
        r = identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'bob@example.com'
          apiAction: 'sent'
        r.then (res) ->
          res.should.not.be.empty
          done()
    describe 'received', ->
      it 'should return messages received by an attribute / identity', (done) ->
        r = identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'bob@example.com'
          apiAction: 'received'
        r.then (res) ->
          res.should.not.be.empty
          done()
    describe 'getname', ->
      it 'should return a cached common name for the attribute'
  describe 'websocket', ->
    it 'should be connected', ->
      socket.connected.should.be.true
    it 'should receive an event when a new message is available', (done) ->
      socket.on 'msg', (e) ->
        done()
        socket._callbacks['$msg'] = []
      m = message.createRating
        author: [['email', 'alice@example.com']]
        recipient: [['email', 'bob@example.com']]
        rating: 10
        context: 'identifi'
      message.sign(m, privKeyPEM, hex)
      r = identifi.request
        method: 'POST'
        apiMethod: 'messages'
        body: m
    it 'should accept and save a message', (done) ->
      socket.emit('msg', { jws: 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjMwNTYzMDEwMDYwNzJhODY0OGNlM2QwMjAxMDYwNTJiODEwNDAwMGEwMzQyMDAwNDllM2JjYjQ5OGRlY2FkYzIwYzRhMDkzMDI2ZGQ4NzgxZWUxMTNhM2VkNjBmZTU4ZGRmNzQ0MWJmZjYyZTA3ZjZmZmQ4ZDE2MjNmZWZiMWUwZDU3NDlhZTg5NjdkNDU2NGQzZDY2NjE3YWQ3Zjk5OTJlMjNiMDVlMjU3ZjQwODUwIn0.eyJhdXRob3IiOltbImVtYWlsIiwibWFydHRpQG1vbmkuY29tIl0sWyJuYW1lIiwiU2F0b3NoaSBOYWthbW90byJdLFsia2V5SUQiLCIvcGJ4alhqd0Vzb2piU2ZkTTN3R1dmRTI0RjRmWDNHYXNtb0hYWTN5WVBNPSJdXSwicmVjaXBpZW50IjpbWyJlbWFpbCIsInNpcml1c0Bpa2kuZmkiXSxbImVtYWlsIiwibWFydHRpQG1vbmkuY29tIl1dLCJ0eXBlIjoidmVyaWZ5X2lkZW50aXR5IiwidGltZXN0YW1wIjoiMjAxNi0wNS0xMFQwOTowNjo1MS4yMzRaIn0.fwQ22hyVeWbBMLdYqnwFT--jfF7l6xPUuCKO-YKMCoqzKvxPOBCRPdLa5qDj2suXPngDzTKp9CmHmRCC3XcbWw', hash: '7A2i/11lDUNH2/srjjiz5X7Dz9Sq7r2QrvLcS76/HDc=' })
      setTimeout ->
        r = identifi.request
          method: 'GET'
          apiMethod: 'messages'
          apiId: '7A2i/11lDUNH2/srjjiz5X7Dz9Sq7r2QrvLcS76/HDc='
        r.then ->
          done()
        return
      , 1000
    it 'should not rebroadcast an already saved message', (done) ->
      socket.on 'msg', (e) ->
        done('Fail!')
        socket._callbacks['$msg'] = []
      socket.emit('msg', { jws: 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjMwNTYzMDEwMDYwNzJhODY0OGNlM2QwMjAxMDYwNTJiODEwNDAwMGEwMzQyMDAwNDllM2JjYjQ5OGRlY2FkYzIwYzRhMDkzMDI2ZGQ4NzgxZWUxMTNhM2VkNjBmZTU4ZGRmNzQ0MWJmZjYyZTA3ZjZmZmQ4ZDE2MjNmZWZiMWUwZDU3NDlhZTg5NjdkNDU2NGQzZDY2NjE3YWQ3Zjk5OTJlMjNiMDVlMjU3ZjQwODUwIn0.eyJhdXRob3IiOltbImVtYWlsIiwibWFydHRpQG1vbmkuY29tIl0sWyJuYW1lIiwiU2F0b3NoaSBOYWthbW90byJdLFsia2V5SUQiLCIvcGJ4alhqd0Vzb2piU2ZkTTN3R1dmRTI0RjRmWDNHYXNtb0hYWTN5WVBNPSJdXSwicmVjaXBpZW50IjpbWyJlbWFpbCIsInNpcml1c0Bpa2kuZmkiXSxbImVtYWlsIiwibWFydHRpQG1vbmkuY29tIl1dLCJ0eXBlIjoidmVyaWZ5X2lkZW50aXR5IiwidGltZXN0YW1wIjoiMjAxNi0wNS0xMFQwOTowNjo1MS4yMzRaIn0.fwQ22hyVeWbBMLdYqnwFT--jfF7l6xPUuCKO-YKMCoqzKvxPOBCRPdLa5qDj2suXPngDzTKp9CmHmRCC3XcbWw', hash: '7A2i/11lDUNH2/srjjiz5X7Dz9Sq7r2QrvLcS76/HDc=' })
      setTimeout ->
        r = identifi.request
          method: 'GET'
          apiMethod: 'messages'
          apiId: '7A2i/11lDUNH2/srjjiz5X7Dz9Sq7r2QrvLcS76/HDc='
        r.then ->
          done()
        return
      , 1000
  describe 'peers', ->
    it 'should have 4 peer addresses', (done) ->
      r = identifi.request
        apiMethod: 'peers'
      r.then (res) ->
        res.length.should.equal 4
        done()
