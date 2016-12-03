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
  before ->
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
  after cleanup
  after ->
    console.log 'Test server at ' + config.get('port') + ' shutting down'
    server.close()
  it 'should return server status', ->
    identifi.request({})
  describe 'wot', ->
    it 'should add a trustIndexedAttribute to maintain identity index for later tests', ->
      m = message.createRating
        author: [['email', 'alice@example.com']]
        recipient: [['keyID', myKey.hash]]
        rating: 10
        context: 'identifi'
      message.sign(m, privKeyPEM, hex)
      identifi.request
        method: 'POST'
        apiMethod: 'messages'
        body: m
      .then ->
        identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'alice@example.com'
          apiAction: 'generatewotindex'
          qs:
            maintain: 1
            depth: 4
          headers:
            'Authorization': 'Bearer ' + identifi.getJwt(privKeyPEM, { admin: true })
      .then (res) ->
        res.should.equal 3
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
      it 'should add rating 1 from david@example.com to bob@example.org', ->
        m = message.createRating
          author: [['email', 'david@example.com']]
          recipient: [['email', 'bob@example.org']]
          rating: 1
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
      it 'add another verification msg', ->
        m = message.create
          author: [['email', 'alice@example.com']]
          recipient: [['email', 'bob@example.com'], ['email', 'bob@example.net']]
          type: 'verify_identity'
        message.sign(m, privKeyPEM, hex)
        r = identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: m
      it 'add another verification msg', ->
        m = message.create
          author: [['email', 'alice@example.com']]
          recipient: [['email', 'bob@example.net'], ['email', 'bob@example.org']]
          type: 'verify_identity'
        message.sign(m, privKeyPEM, hex)
        r = identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: m
      it 'add another verification msg', ->
        m = message.create
          author: [['email', 'bob@example.com']]
          recipient: [['email', 'charles@example.com'], ['url', 'http://twitter.com/charles']]
          type: 'verify_identity'
        message.sign(m, privKeyPEM, hex)
        r = identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: m
      it 'should add an unsigned and authorless message when authorization is provided', ->
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
    describe 'retrieve', ->
      it 'should fail if the message was not found', ->
        r = identifi.request
              apiMethod: 'messages'
              apiId: '1234'
        r.should.be.rejected
      it 'should return the previously saved message', ->
        r = identifi.request
          apiMethod: 'messages'
          apiId: m.hash
        r.then (res) ->
          res.hash.should.equal m.hash
    describe 'non-public messages', ->
      privMsg = null
      it 'should add a non-public msg', ->
        privMsg = message.create
          author: [['email', 'bob@example.com']]
          recipient: [['email', 'darwin@example.com'], ['url', 'http://twitter.com/darwin']]
          type: 'verify_identity'
          public: false
        message.sign(privMsg, privKeyPEM, hex)
        identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: privMsg
      it 'should not be visible in message listing', ->
        r = identifi.request
          apiMethod: 'messages'
          apiId: privMsg.hash
        r.should.be.rejected
      it 'should not be returned by hash', ->
        identifi.request
          apiMethod: 'messages'
        .then (res) ->
          res[0].hash.should.not.equal privMsg.hash
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
      it 'should generate a web of trust index', ->
        r = identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'alice@example.com'
          apiAction: 'generatewotindex'
          qs:
            depth: 4
          headers:
            'Authorization': 'Bearer ' + identifi.getJwt(privKeyPEM, { admin: true })
        r.then (res) ->
          res.should.equal 7
      it 'should generate a web of trust index for a keyID', ->
        # This message is deleted in a test later
        m = message.createRating
          author: [['keyID', myKey.hash]]
          recipient: [['email', 'alice@example.com']]
          rating: 10
          context: 'identifi'
        message.sign(m, privKeyPEM, hex)
        r = identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: m
        r.then ->
          r = identifi.request
            apiMethod: 'identities'
            apiIdType: 'keyID'
            apiId: myKey.hash
            apiAction: 'generatewotindex'
            qs:
              depth: 4
            headers:
              'Authorization': 'Bearer ' + identifi.getJwt(privKeyPEM, { admin: true })
          r.then (res) ->
            res.should.equal 6
    describe 'list', ->
      it 'should list messages ordered by date', ->
        r = identifi.request
          apiMethod: 'messages'
        r.then (res) ->
          res[0].hash.should.equal m.hash
      it 'should filter messages by type', ->
        r = identifi.request
          apiMethod: 'messages'
          qs:
            type: 'rating'
        r.then (res) ->
          msg.type.should.equal 'rating' for msg in res
      it 'should filter messages by rating type positive', ->
        r = identifi.request
          apiMethod: 'messages'
          qs:
            type: 'rating:positive'
        r.then (res) ->
          msg.type.should.equal 'rating' for msg in res
          msg.rating.should.be.above (msg.max_rating + msg.min_rating) / 2
      it 'should filter messages by rating type negative', ->
        r = identifi.request
          apiMethod: 'messages'
          qs:
            type: 'rating:negative'
        r.then (res) ->
          msg.type.should.equal 'rating' for msg in res
          msg.rating.should.be.below (msg.max_rating + msg.min_rating) / 2
      it 'should filter messages by viewpoint, max_distance 1', ->
        r = identifi.request
          apiMethod: 'messages'
          qs:
            viewpoint_name: 'email'
            viewpoint_value: 'alice@example.com'
            max_distance: 1
        r.then (res) ->
          res.length.should.equal 8
      it 'should filter messages by viewpoint, max_distance 2', ->
        r = identifi.request
          apiMethod: 'messages'
          qs:
            viewpoint_name: 'email'
            viewpoint_value: 'alice@example.com'
            max_distance: 2
        r.then (res) ->
          res.length.should.equal 10
      it 'should filter messages by timestamp_lte', ->
        r = identifi.request
          apiMethod: 'messages'
          qs:
            timestamp_lte: m.signedData.timestamp
        r.then (res) ->
          res.length.should.equal 12
      it 'should filter messages by timestamp_gte', ->
        r = identifi.request
          apiMethod: 'messages'
          qs:
            timestamp_gte: m.signedData.timestamp
        r.then (res) ->
          res.length.should.equal 1
    describe 'delete', ->
      it 'should fail without auth', ->
        r = identifi.request
          apiMethod: 'messages'
          apiId: m.hash
          method: 'DELETE'
        r.should.be.rejected
      it 'should delete the previously saved message', ->
        r = identifi.request
          apiMethod: 'messages'
          apiId: m.hash
          method: 'DELETE'
          headers:
            'Authorization': 'Bearer ' + identifi.getJwt(privKeyPEM, { admin: true })
        r.then (res) ->
          res.should.equal 'OK'
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
      it 'should return an empty set if an identity was not found', ->
        r = identifi.request
              apiMethod: 'identities'
              apiId: 'bob@example.com'
              apiIdType: 'nope'
        r.then (res) ->
          res.should.be.empty
    describe 'retrieve', ->
      it 'should return a cached identity', ->
        r = identifi.request
          apiMethod: 'identities'
          apiId: 'bob@example.com'
          apiIdType: 'email'
        r.then (res) ->
          json_res = JSON.stringify(res)
          json_res.should.contain 'bob@example.com'
          json_res.should.contain 'bob@example.net'
          json_res.should.contain 'bob@example.org'
      it 'should return the same identity', ->
        r = identifi.request
          apiMethod: 'identities'
          apiId: 'bob@example.org'
          apiIdType: 'email'
        r.then (res) ->
          json_res = JSON.stringify(res)
          json_res.should.contain 'bob@example.com'
          json_res.should.contain 'bob@example.net'
          json_res.should.contain 'bob@example.org'
    describe 'list', ->
      ### it 'should return identities', ->
        r = identifi.request
          apiMethod: 'identities'
          qs:
            viewpoint_name: 'email'
            viewpoint_value: 'alice@example.com'
        r.then (res) ->
          console.log(res)
          res.length.should.equal 5
          ###
      it 'should filter identities by attribute name', ->
        r = identifi.request
          apiMethod: 'identities'
          qs:
            attr_name: 'email'
        r.then (res) ->
          res.length.should.equal 3
      it 'should filter by search query', -> # TODO: fix?
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
      it 'should return a list of peers as identifi identities', ->
        r = identifi.request
          apiMethod: 'identities'
          qs:
            attr_name: 'identifi_node'
        r.then (res) ->
          res.length.should.equal 0
    describe 'verifications', ->
      it 'should return an identity, i.e. set of attributes connected to the query param', ->
        r = identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'bob@example.com'
          apiAction: 'verifications'
          qs:
            viewpoint_name: 'email'
            viewpoint_value: 'alice@example.com'
        r.then (res) ->
          json_res = JSON.stringify(res)
          # json_res.should.contain 'bob@example.com'
          json_res.should.contain 'bob@example.net'
          json_res.should.contain 'bob@example.org'
          json_res.should.contain 'Bob the Builder'
      it 'should return the same identity', ->
        r = identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'bob@example.org'
          apiAction: 'verifications'
          qs:
            viewpoint_name: 'email'
            viewpoint_value: 'alice@example.com'
        r.then (res) ->
          json_res = JSON.stringify(res)
          json_res.should.contain 'bob@example.com'
          json_res.should.contain 'bob@example.net'
          # json_res.should.contain 'bob@example.org'
          # json_res.should.contain 'Bob the Builder' # TODO: fix
    describe 'connecting_msgs', ->
        it 'should return messages that connect id1 and id2 to the same identity', ->
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
    describe 'stats', ->
      it 'should return the stats of an attribute, no viewpoint', ->
        r = identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'bob@example.com'
          apiAction: 'stats'
        r.then (res) ->
          res.sent_positive.should.equal 1
          res.sent_neutral.should.equal 0
          res.sent_negative.should.equal 0
          res.received_positive.should.equal 1
          res.received_neutral.should.equal 0
          res.received_negative.should.equal 2
          res.first_seen.should.not.be.empty
      it 'should return the stats of an attribute, using a viewpoint & max_distance 1', ->
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
          return # TODO: temporarily disabled
          res.sent_positive.should.equal 1
          res.sent_neutral.should.equal 0
          res.sent_negative.should.equal 0
          res.received_positive.should.equal 1
          res.received_neutral.should.equal 0
          res.received_negative.should.equal 0
          res.first_seen.should.not.be.empty
      it 'should return the stats of an attribute, using a viewpoint & max_distance 2', ->
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
          return # TODO: temporarily disabled
          res.sent_positive.should.equal 1
          res.sent_neutral.should.equal 0
          res.sent_negative.should.equal 0
          res.received_positive.should.equal 1
          res.received_neutral.should.equal 0
          res.received_negative.should.equal 1
          res.first_seen.should.not.be.empty
    describe 'sent', ->
      it 'should return messages sent by an attribute / identity', ->
        r = identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'bob@example.com'
          apiAction: 'sent'
        r.then (res) ->
          res.should.not.be.empty
    describe 'received', ->
      it 'should return messages received by an attribute / identity', ->
        r = identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'bob@example.com'
          apiAction: 'received'
        r.then (res) ->
          res.should.not.be.empty
      it 'should list all messages received by the identity that bob@example.org points to, as perceived by alice@example.com', ->
        r = identifi.request
          apiMethod: 'identities'
          apiIdType: 'email'
          apiId: 'bob@example.org'
          apiAction: 'received'
          qs:
            viewpoint_name: 'email'
            viewpoint_value: 'alice@example.com'
        r.then (res) ->
          res.length.should.equal 6
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
      return
    it 'should accept and save a message', (done) ->
      socket.emit('msg', { jws: 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjMwNTYzMDEwMDYwNzJhODY0OGNlM2QwMjAxMDYwNTJiODEwNDAwMGEwMzQyMDAwNDllM2JjYjQ5OGRlY2FkYzIwYzRhMDkzMDI2ZGQ4NzgxZWUxMTNhM2VkNjBmZTU4ZGRmNzQ0MWJmZjYyZTA3ZjZmZmQ4ZDE2MjNmZWZiMWUwZDU3NDlhZTg5NjdkNDU2NGQzZDY2NjE3YWQ3Zjk5OTJlMjNiMDVlMjU3ZjQwODUwIn0.eyJhdXRob3IiOltbImVtYWlsIiwibWFydHRpQG1vbmkuY29tIl0sWyJuYW1lIiwiU2F0b3NoaSBOYWthbW90byJdLFsia2V5SUQiLCIvcGJ4alhqd0Vzb2piU2ZkTTN3R1dmRTI0RjRmWDNHYXNtb0hYWTN5WVBNPSJdXSwicmVjaXBpZW50IjpbWyJlbWFpbCIsInNpcml1c0Bpa2kuZmkiXSxbImVtYWlsIiwibWFydHRpQG1vbmkuY29tIl1dLCJ0eXBlIjoidmVyaWZ5X2lkZW50aXR5IiwidGltZXN0YW1wIjoiMjAxNi0wNS0xMFQwOTowNjo1MS4yMzRaIn0.fwQ22hyVeWbBMLdYqnwFT--jfF7l6xPUuCKO-YKMCoqzKvxPOBCRPdLa5qDj2suXPngDzTKp9CmHmRCC3XcbWw', hash: '7A2i/11lDUNH2/srjjiz5X7Dz9Sq7r2QrvLcS76/HDc=' })
      setTimeout ->
        r = identifi.request
          method: 'GET'
          apiMethod: 'messages'
          apiId: '7A2i/11lDUNH2/srjjiz5X7Dz9Sq7r2QrvLcS76/HDc='
        r.then ->
          done()
      , 1000
      return
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
      return
  describe 'peers', ->
    it 'should have 4 peer addresses', (done) ->
      r = identifi.request
        apiMethod: 'peers'
      r.then (res) ->
        res.length.should.equal 3
        done()
      return
