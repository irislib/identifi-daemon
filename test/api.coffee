###global describe, it, after, before ###

'use strict'
process.env.NODE_ENV = 'test'
fs = require('fs')
request = require('request')
config = require('config')
identifi = require('identifi-lib')
chai = require('chai')
chaiAsPromised = require('chai-as-promised')
chai.should()
chai.use chaiAsPromised

cleanup = ->
  fs.unlink './identifi_test.db', (err) ->
  fs.unlink './identifi_test.log', (err) ->

describe 'API', ->
  server = undefined
  before ->
    cleanup()
    # After hook fails to execute when errors are thrown
    server = require('../server.js')
    identifi.host = 'http://localhost:8081/api/'
  after cleanup
  after ->
    console.log 'Test server at ' + config.get('port') + ' shutting down'
    server.close()
  it 'should return server status', (done) ->
    identifi.get_status().should.eventually.notify done
  it 'should return a list of peers', (done) ->
    identifi.get_peers().should.eventually.notify done
  describe 'Search', ->
  describe 'Overview', ->
  describe 'Connections', ->
  describe 'Sent', ->
  describe 'Received', ->
