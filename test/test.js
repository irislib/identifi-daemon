/*global describe, it, after, before */
'use strict';
var fs = require('fs');
var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
chai.should();
chai.use(chaiAsPromised);

var Message = require('../message.js');

var cleanup = function() {
  fs.unlink('./test.db');
};

describe('Database', function () {
  var db;
  before(function() {
    cleanup(); // After hook fails to execute when errors are thrown
    var knex = require('knex')({
      dialect: 'sqlite3',
      debug: true,
      connection: {
        filename: './test.db'
      }
    });
    db = require('../db.js')(knex);
  });

  after(cleanup);

  it('should initially have 0 messages', function (done) {
    db.getMessageCount().then(function(res) {
      res[0].val.should.equal(0);
      done();
    });
  });

  it('should save a message', function (done) {
    var message = Message.create({ type: 'rating', author: [['email', 'alice@example.com']], recipient: [['email', 'bob@example.com']], message: 'Positive', rating: 1 });
    Message.sign(message, 'pubkey');
    db.saveMessage(message).should.eventually.notify(done);
  });

  it('should save another message', function (done) {
    var message = Message.create({ type: 'rating', author: [['email', 'charles@example.com']], recipient: [['email', 'bob@example.com']], message: 'Positive', rating: -1 });
    Message.sign(message, 'pubkey');
    db.saveMessage(message).should.eventually.notify(done);
  });

  it('should have 2 message', function (done) {
    db.getMessageCount().then(function(res) {
      res[0].val.should.equal(2);
      done();
    });
  });

  it('should return sent messages', function (done) {
    db.getSent(['email', 'alice@example.com']).then(function(res) {
      res.length.should.equal(1);
      done();
    });
  });

  it('should return received messages', function (done) {
    db.getReceived(['email', 'bob@example.com']).then(function(res) {
      res.length.should.equal(2);
      done();
    });
  });
});
