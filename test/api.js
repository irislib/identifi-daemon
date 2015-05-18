/*global describe, it, after, before */
'use strict';
process.env.NODE_ENV = 'test'
var fs = require('fs');
var request = require('request');
var config = require('config');

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
  });

  after(cleanup);
  after(function() {
    console.log('Test server at ' + config.get('port') + ' shutting down');
    server.close();    
  });

  it('should respond at /api', function (done) {
    request('http://localhost:' + config.get('port') + '/api', function (error, response, body) {
      if (!error && response.statusCode == 200) {
        console.log(body);
        done();
      }
    });
  });
});