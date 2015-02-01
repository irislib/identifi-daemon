/*jshint unused: false */
'use strict';
var crypto = require('crypto');

var algorithm = 'ecdsa-with-SHA1';
var encoding = 'base64';

var getHash = function(msg) {
  return crypto.createHash('sha256').update(JSON.stringify(msg.signedData)).digest();
};

module.exports = { 
  create: function(signedData, publish) {
    var msg = {
      signedData: signedData || {},
      isPublished: publish || false, 
    };

    msg.signedData.timestamp = msg.signedData.timestamp || Date.now();
    msg.hash = getHash(msg).toString(encoding);

    return msg;
  },

  parse: function(json) {
    var msg = JSON.parse(json);
    if (!msg.signedData) {
      throw new Error("Missing signedData");
    }
    if (!msg.signature) {
      throw new Error("Missing signature");
    }
    if (!this.verify(msg)) {
      throw new Error("Invalid signature");
    }

    return false;
  },

  sign: function(msg, key) {
    var privKey = '-----BEGIN EC PRIVATE KEY-----\n'+
'MHQCAQEEINY+49rac3jkC+S46XN0f411svOveILjev4R3aBehwUKoAcGBSuBBAAK\n'+
'oUQDQgAEKn3lQ3+/aN6xNd9DSFrYbaPSGOzLMbb1kQZ9lCMtwc6Og4hfCMLhaSbE\n'+
'3sXek8e2fvKrTp8FY1MyCL4qMeVviA==\n'+
'-----END EC PRIVATE KEY-----';
    var pubKey = '-----BEGIN PUBLIC KEY-----\n'+
'MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEKn3lQ3+/aN6xNd9DSFrYbaPSGOzLMbb1\n'+
'kQZ9lCMtwc6Og4hfCMLhaSbE3sXek8e2fvKrTp8FY1MyCL4qMeVviA==\n'+
'-----END PUBLIC KEY-----';
    var signer = crypto.createSign(algorithm);
    signer.update(JSON.stringify(msg.signedData));
    var signature = signer.sign(privKey, encoding);
    msg.signature = { signerPubkey: pubKey, signature: signature };
    console.log(msg);
  },

  verify: function(msg) {
    var verifier = crypto.createVerify(algorithm);
    verifier.update(JSON.stringify(msg.signedData));
    return verifier.verify(msg.signature.signerPubkey, msg.signature.signature, encoding);
  }
};
