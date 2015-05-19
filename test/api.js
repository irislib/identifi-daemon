/*global describe, it, after, before */
'use strict';
process.env.NODE_ENV = 'test'
var fs = require('fs');
var request = require('request');
var config = require('config');
var identifi = require('identifi-lib');

var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
chai.should();
chai.use(chaiAsPromised);

var cleanup = function() {
  fs.unlink('./identifi_test.db', function(err) {});
  fs.unlink('./identifi_test.log', function(err) {});
};

describe('API', function () {
  var server;
  before(function() {
    cleanup(); // After hook fails to execute when errors are thrown
    server = require('../server.js');
    identifi.host = 'http://localhost:8081/api/';
  });

  after(cleanup);
  after(function() {
    console.log('Test server at ' + config.get('port') + ' shutting down');
    server.close();    
  });

  it('should return server info', function (done) {
    identifi.get_info().should.eventually.notify(done);
  });

  it('should return a list of peers', function (done) {
    identifi.get_peers().should.eventually.notify(done);
  });

  describe('Search', function() {

  });

  describe('Overview', function() {

  });

  describe('Connections', function() {

  });

  describe('Sent', function() {

  });

  describe('Received', function() {

  });
});