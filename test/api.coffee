###global describe, it, after, before ###

'use strict'
process.env.NODE_ENV = 'test'
fs = require('fs')
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

describe 'API', ->
  server = undefined
  socket = undefined
  before (done) ->
    cleanup()
    # After hook fails to execute when errors are thrown
    server = require('../server.js')

    myKey = keyutil.getDefault(datadir)
    privKeyPEM = myKey.private.pem
    hex = myKey.public.hex

    identifi.apiRoot = 'http://127.0.0.1:4944/api'
    server.ready.then ->
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
      it 'add a connection msg', ->
        m = message.create
          author: [['email', 'alice@example.com']]
          recipient: [['email', 'bob@example.com'], ['name', 'Bob the Builder']]
          type: 'verify_identity'
        message.sign(m, privKeyPEM, hex)
        r = identifi.request
          method: 'POST'
          apiMethod: 'messages'
          body: m
      it 'add another connection msg', (done) ->
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
        .then ->
          r = identifi.request
            apiMethod: 'messages'
            apiId: privMsg.hash
          r.should.be.rejected
          identifi.request
            apiMethod: 'messages'
          .then (res) ->
            res[0].hash.should.not.equal privMsg.hash
            done()
    describe 'trustpaths', ->
      it 'should return a trustpath from alice to bob', (done) ->
        r = identifi.request
          apiMethod: 'id'
          apiIdType: 'email'
          apiId: 'alice@example.com'
          apiAction: 'trustpaths'
          qs:
            target_name: 'email'
            target_value: 'bob@example.com'
        r.then (res) ->
          res.should.not.be.empty
          res[0].path_string.split(':').length.should.equal 5
          done()
      it 'should return a trustpath from alice to david', (done) ->
        r = identifi.request
          apiMethod: 'id'
          apiIdType: 'email'
          apiId: 'alice@example.com'
          apiAction: 'trustpaths'
          qs:
            target_name: 'email'
            target_value: 'david@example.com'
        r.then (res) ->
          res.should.not.be.empty
          res[0].path_string.split(':').length.should.equal 9
          done()
      it 'should not generate a web of trust without auth', ->
        r = identifi.request
          apiMethod: 'id'
          apiIdType: 'email'
          apiId: 'alice@example.com'
          apiAction: 'generatewotindex'
          qs:
            depth: 3
        r.should.be.rejected
      it 'should generate a web of trust index', (done) ->
        r = identifi.request
          apiMethod: 'id'
          apiIdType: 'email'
          apiId: 'alice@example.com'
          apiAction: 'generatewotindex'
          qs:
            depth: 3
          headers:
            'Authorization': 'Bearer ' + identifi.getJwt(privKeyPEM)
        r.then (res) ->
          res.should.not.be.empty
          res[0].wot_size.should.equal 3
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
      it 'should filter messages by viewpoint, max_distance 1', (done) ->
        r = identifi.request
          apiMethod: 'messages'
          qs:
            viewpoint_name: 'email'
            viewpoint_value: 'alice@example.com'
            max_distance: 1
        r.then (res) ->
          res.length.should.equal 4
          done()
      it 'should filter messages by timestamp_lte', (done) ->
        console.log m.signedData.timestamp
        r = identifi.request
          apiMethod: 'messages'
          qs:
            timestamp_lte: m.signedData.timestamp
        r.then (res) ->
          res.length.should.equal 7
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
          res.length.should.equal 6
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
            'Authorization': 'Bearer ' + identifi.getJwt(privKeyPEM)
        r.then (res) ->
          res.should.equal 'OK'
          done()
      it 'should fail if the message was not found', ->
        r = identifi.request
          apiMethod: 'messages'
          apiId: '1234'
          method: 'DELETE'
          headers:
            'Authorization': 'Bearer ' + identifi.getJwt(privKeyPEM)
        r.should.be.rejected
      it 'should have removed the message', ->
        r = identifi.request
              apiMethod: 'messages'
              apiId: m.hash
        r.should.be.rejected
  describe 'attributes', ->
      it 'should return an empty set if an identity was not found', (done) ->
        r = identifi.request
              apiMethod: 'id'
              apiId: 'bob@example.com'
              apiIdType: 'nope'
        r.then (res) ->
          res.should.be.empty
          done()
    describe 'retrieve', ->
      it 'should return an identity', (done) ->
        r = identifi.request
          apiMethod: 'id'
          apiId: 'bob@example.com'
          apiIdType: 'email'
        r.then (res) ->
          res.should.not.be.empty
          done()
    describe 'list', ->
      it 'should return identities', (done) ->
        r = identifi.request
          apiMethod: 'id'
        r.then (res) ->
          res.length.should.equal 9
          done()
      it 'should filter identities by attribute name', (done) ->
        r = identifi.request
          apiMethod: 'id'
          qs:
            name: 'email'
        r.then (res) ->
          res.length.should.equal 5
          done()
      it 'should filter by search query', (done) ->
        r = identifi.request
          apiMethod: 'id'
          qs:
            search_value: 'i'
        r.then (res) ->
          res.length.should.equal 4
          res[0].value.should.equal 'Bob the Builder'
          res[1].value.should.equal 'alice@example.com'
          res[2].value.should.equal 'david@example.com'
          res[3].value.should.equal 'http://twitter.com/charles'
          done()
      it 'should return a list of peers as identifi identities', (done) ->
        r = identifi.request
          apiMethod: 'id'
          qs:
            name: 'identifi_node'
        r.then (res) ->
          res.length.should.equal 0
          done()
    describe 'connections', ->
      it 'should return an identity, i.e. set of attributes connected to the query param', (done) ->
        r = identifi.request
          apiMethod: 'id'
          apiIdType: 'email'
          apiId: 'bob@example.com'
          apiAction: 'connections'
          qs:
            viewpoint_name: 'email'
            viewpoint_value: 'alice@example.com'
        r.then (res) ->
          res.should.not.be.empty
          done()
    describe 'connecting_msgs', ->
        it 'should return messages that connect id1 and id2 to the same identity', (done) ->
          r = identifi.request
            apiMethod: 'id'
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
          apiMethod: 'id'
          apiIdType: 'email'
          apiId: 'bob@example.com'
          apiAction: 'stats'
        r.then (res) ->
          res.length.should.equal 1
          res[0].sentPositive.should.equal 1
          res[0].sentNeutral.should.equal 0
          res[0].sentNegative.should.equal 0
          res[0].receivedPositive.should.equal 1
          res[0].receivedNeutral.should.equal 0
          res[0].receivedNegative.should.equal 2
          res[0].firstSeen.should.not.be.empty
          done()
      it 'should return the stats of an attribute, using a viewpoint & max_distance 1', (done) ->
        r = identifi.request
          apiMethod: 'id'
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
          res[0].sentPositive.should.equal 1
          res[0].sentNeutral.should.equal 0
          res[0].sentNegative.should.equal 0
          res[0].receivedPositive.should.equal 1
          res[0].receivedNeutral.should.equal 0
          res[0].receivedNegative.should.equal 0
          res[0].firstSeen.should.not.be.empty
          done()
        .catch (e) ->
          done(e)
      it 'should return the stats of an attribute, using a viewpoint & max_distance 2', (done) ->
        r = identifi.request
          apiMethod: 'id'
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
          res[0].sentPositive.should.equal 1
          res[0].sentNeutral.should.equal 0
          res[0].sentNegative.should.equal 0
          res[0].receivedPositive.should.equal 1
          res[0].receivedNeutral.should.equal 0
          res[0].receivedNegative.should.equal 1
          res[0].firstSeen.should.not.be.empty
          done()
        .catch (e) ->
          done(e)
    describe 'sent', ->
      it 'should return messages sent by an attribute / identity', (done) ->
        r = identifi.request
          apiMethod: 'id'
          apiIdType: 'email'
          apiId: 'bob@example.com'
          apiAction: 'sent'
        r.then (res) ->
          res.should.not.be.empty
          done()
    describe 'received', ->
      it 'should return messages received by an attribute / identity', (done) ->
        r = identifi.request
          apiMethod: 'id'
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
      socket.emit('msg', { jws: 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjMwNTYzMDEwMDYwNzJhODY0OGNlM2QwMjAxMDYwNTJiODEwNDAwMGEwMzQyMDAwNDllM2JjYjQ5OGRlY2FkYzIwYzRhMDkzMDI2ZGQ4NzgxZWUxMTNhM2VkNjBmZTU4ZGRmNzQ0MWJmZjYyZTA3ZjZmZmQ4ZDE2MjNmZWZiMWUwZDU3NDlhZTg5NjdkNDU2NGQzZDY2NjE3YWQ3Zjk5OTJlMjNiMDVlMjU3ZjQwODUwIn0.eyJhdXRob3IiOltbImtleUlEIiwiL3BieGpYandFc29qYlNmZE0zd0dXZkUyNEY0ZlgzR2FzbW9IWFkzeVlQTT0iXV0sInJlY2lwaWVudCI6W1sia2V5SUQiLCJOSzBSNjhLelJGRk9acThtSHN5dTdHTDFqdEpYUzdMRmRBVFB5WGtNQmIwPSJdXSwiY29tbWVudCI6IkFuIElkZW50aWZpIHNlZWQgbm9kZSwgdHJ1c3RlZCBieSBkZWZhdWx0IiwicmF0aW5nIjoxMCwiY29udGV4dCI6ImlkZW50aWZpX25ldHdvcmsiLCJwdWJsaWMiOmZhbHNlLCJ0aW1lc3RhbXAiOiIyMDE2LTA0LTIzVDE5OjE1OjMyLjI3OVoiLCJ0eXBlIjoicmF0aW5nIiwibWF4UmF0aW5nIjoxMCwibWluUmF0aW5nIjotMTB9.8hGtWj_EHk4LzD5brtmOmoynqpFiRlLlaX-HaZzkGPu432zHETz7NlAzJzod_uNrOy-74snDfs-j--RdXcyDCw', hash: 'Z3D1NdcM9XA/tmDZceSPuGozQAXAAdBK7RB2QI7GWVU=' })
      setTimeout ->
        r = identifi.request
          method: 'GET'
          apiMethod: 'messages'
          apiId: 'Z3D1NdcM9XA/tmDZceSPuGozQAXAAdBK7RB2QI7GWVU='
        r.then ->
          done()
        return
      , 1000
    it 'should not rebroadcast an already saved message', (done) ->
      socket.on 'msg', (e) ->
        done('Fail!')
        socket._callbacks['$msg'] = []
      socket.emit('msg', { jws: 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjMwNTYzMDEwMDYwNzJhODY0OGNlM2QwMjAxMDYwNTJiODEwNDAwMGEwMzQyMDAwNDllM2JjYjQ5OGRlY2FkYzIwYzRhMDkzMDI2ZGQ4NzgxZWUxMTNhM2VkNjBmZTU4ZGRmNzQ0MWJmZjYyZTA3ZjZmZmQ4ZDE2MjNmZWZiMWUwZDU3NDlhZTg5NjdkNDU2NGQzZDY2NjE3YWQ3Zjk5OTJlMjNiMDVlMjU3ZjQwODUwIn0.eyJhdXRob3IiOltbImtleUlEIiwiL3BieGpYandFc29qYlNmZE0zd0dXZkUyNEY0ZlgzR2FzbW9IWFkzeVlQTT0iXV0sInJlY2lwaWVudCI6W1sia2V5SUQiLCJOSzBSNjhLelJGRk9acThtSHN5dTdHTDFqdEpYUzdMRmRBVFB5WGtNQmIwPSJdXSwiY29tbWVudCI6IkFuIElkZW50aWZpIHNlZWQgbm9kZSwgdHJ1c3RlZCBieSBkZWZhdWx0IiwicmF0aW5nIjoxMCwiY29udGV4dCI6ImlkZW50aWZpX25ldHdvcmsiLCJwdWJsaWMiOmZhbHNlLCJ0aW1lc3RhbXAiOiIyMDE2LTA0LTIzVDE5OjE1OjMyLjI3OVoiLCJ0eXBlIjoicmF0aW5nIiwibWF4UmF0aW5nIjoxMCwibWluUmF0aW5nIjotMTB9.8hGtWj_EHk4LzD5brtmOmoynqpFiRlLlaX-HaZzkGPu432zHETz7NlAzJzod_uNrOy-74snDfs-j--RdXcyDCw', hash: 'Z3D1NdcM9XA/tmDZceSPuGozQAXAAdBK7RB2QI7GWVU=' })
      setTimeout ->
        r = identifi.request
          method: 'GET'
          apiMethod: 'messages'
          apiId: 'Z3D1NdcM9XA/tmDZceSPuGozQAXAAdBK7RB2QI7GWVU='
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
