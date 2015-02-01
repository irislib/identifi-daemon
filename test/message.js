/*global describe, it, after, before */
'use strict';
var crypto = require('crypto');
var Message = require('../message.js');

var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
chai.should();
chai.use(chaiAsPromised);

describe('Message', function () {
  describe('Create method', function() {
    var msg;

    before(function() {
      msg = Message.create();
    });

    it('should create a message', function() {
      msg.should.have.deep.property('signedData.timestamp');
      msg.should.have.property('isPublished');
      msg.should.have.property('hash');
    });
  });

  describe('Message signature', function() {
    var msg, privKey, pubKey;

    before(function() {
      msg = Message.create();
      privKey = '-----BEGIN EC PRIVATE KEY-----\n'+
        'MHQCAQEEINY+49rac3jkC+S46XN0f411svOveILjev4R3aBehwUKoAcGBSuBBAAK\n'+
        'oUQDQgAEKn3lQ3+/aN6xNd9DSFrYbaPSGOzLMbb1kQZ9lCMtwc6Og4hfCMLhaSbE\n'+
        '3sXek8e2fvKrTp8FY1MyCL4qMeVviA==\n'+
        '-----END EC PRIVATE KEY-----';
      pubKey = 'MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEKn3lQ3+/aN6xNd9DSFrYbaPSGOzLMbb1kQZ9lCMtwc6Og4hfCMLhaSbE3sXek8e2fvKrTp8FY1MyCL4qMeVviA==';
    });

    it('should be created with sign()', function() {
      Message.sign(msg, privKey, pubKey);
      msg.should.have.property('signature');
      msg.should.have.deep.property('signature.signerPubkey');
      msg.should.have.deep.property('signature.signature');
    });

    it('should be accepted by verify()', function() {
      Message.verify(msg);
    });
  });

  describe('Parse method', function() {
    it('should create a message from valid data', function() {
      var data = {
        signedData: { timestamp: 1422822269531 },
        signature: {
          signerPubkey: 'MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEKn3lQ3+/aN6xNd9DSFrYbaPSGOzLMbb1kQZ9lCMtwc6Og4hfCMLhaSbE3sXek8e2fvKrTp8FY1MyCL4qMeVviA==',
          signature: 'MEUCIDywg79Jes1BvEgCGz/FGMRRusCNVpiXEVigLf6nwn42AiEAg4xfFgoHOeW0Yl11uAtEWT7BsQLFQyAAjGPA9U+Z6+M='
        }
      };
      var msg = Message.parse(JSON.stringify(data));
    });

    it('should not accept a message without signedData', function() {
      var data = {
        signature: {
          signerPubkey: '',
          signature: ''
        }
      };
      var f = function() {
        Message.parse(JSON.stringify(data));
      };
      f.should.throw(Error);
    });

    it('should not accept a message without a signature', function() {
      var data = {
        signedData: {}
      };
      var f = function() {
        Message.parse(JSON.stringify(data));
      };
      f.should.throw(Error);
    });

    it('should not accept a message with an invalid signature', function() {
      var data = {
        signedData: { timestamp: 1422822269531 },
        signature: {
          signerPubkey: 'MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEKn3lQ3+/aN6xNd9DSFrYbaPSGOzLMbb1kQZ9lCMtwc6Og4hfCMLhaSbE3sXek8e2fvKrTp8FY1MyCL4qMeVviA==',
          signature: 'MEUCIDywg79Jes1BvEgCGz/FGMRRusCNVpiXEVigLf6nwn42AiEAg4xfFgoHOeW0Yl11uAtEWT7BsQLFQyAAjGPA9U+Z6+L='
        }
      };
      var f = function() {
        var msg = Message.parse(JSON.stringify(data));
      };
      f.should.throw(Error);
    });
  });
});
