/*global describe, it, after, before */
'use strict';
var crypto = require('crypto');
var Message = require('../message.js');

var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
chai.should();
chai.use(chaiAsPromised);

describe('Message', function () {
  describe('createRating method', function() {
    var msg;

    before(function() {
      msg = Message.createRating(
        [['email', 'alice@example.com']],
        [['email', 'bob@example.com']],
        5,
        'Good guy'
        );
    });

    it('should create a message', function() {
      msg.should.have.deep.property('signedData.timestamp');
      msg.should.have.property('isPublic');
    });
  });

  describe('Validate method', function() {
    it('should not accept a message without signedData', function() {
      var data = {
        signedData: {}
      };
      var f = function() {
        Message.validate(JSON.stringify(data));
      }
      f.should.throw(Error);
    });
  });

  describe('Message signature', function() {
    var msg, privKey, pubKey;

    before(function() {
      msg = Message.createRating(
        [['email', 'alice@example.com']],
        [['email', 'bob@example.com']],
        5,
        'Good guy'
        );

      privKey = '-----BEGIN EC PRIVATE KEY-----\n'+
        'MHQCAQEEINY+49rac3jkC+S46XN0f411svOveILjev4R3aBehwUKoAcGBSuBBAAK\n'+
        'oUQDQgAEKn3lQ3+/aN6xNd9DSFrYbaPSGOzLMbb1kQZ9lCMtwc6Og4hfCMLhaSbE\n'+
        '3sXek8e2fvKrTp8FY1MyCL4qMeVviA==\n'+
        '-----END EC PRIVATE KEY-----';
      pubKey = '-----BEGIN PUBLIC KEY-----\n'+
        'MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEKn3lQ3+/aN6xNd9DSFrYbaPSGOzLMbb1\n'+
        'kQZ9lCMtwc6Og4hfCMLhaSbE3sXek8e2fvKrTp8FY1MyCL4qMeVviA==\n'+
        '-----END PUBLIC KEY-----'
    });

    it('should be created with sign()', function() {
      Message.sign(msg, privKey, 'someKeyID');
      msg.should.have.property('jws');
      msg.should.have.property('jwsHeader');
      msg.should.have.property('hash');
    });

    it('should be accepted by verify()', function() {
      Message.verify(msg, pubKey);
    });
  });

  describe('Deserialize method', function() {
    it('should not accept invalid data', function() {
      var jws = 'asdf';
      var f = function() {
        Message.deserialize(jws);
      };
      f.should.throw(Error);
    });
  });
});
