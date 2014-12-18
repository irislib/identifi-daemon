/*global describe, it, after */
'use strict';
var fs = require('fs');
var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
chai.should();
chai.use(chaiAsPromised);

var knex = require('knex')({
  dialect: 'sqlite3',
  connection: {
    filename: './test.db'
  }
});
var db = require('../db.js')(knex);

describe('Database', function () {
    after(function() { fs.unlink('test.db');  });

    it('should initially have 0 messages', function (done) {
        db.getMessageCount().then(function(res) {
          res[0].val.should.equal(0);
          done();
        });
    });

    it('should save a message', function (done) {
        db.saveMessage({}).should.eventually.notify(done);
    });

    it('should have 1 message', function (done) {
        db.getMessageCount().then(function(res) {
          res[0].val.should.equal(1);
          done();
        });
    });
});
