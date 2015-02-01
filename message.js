/*jshint unused: false */
'use strict';
var crypto = require('crypto');

var algorithm = 'ecdsa-with-SHA1';
var encoding = 'base64';

var getHash = function(msg) {
  return crypto.createHash('sha256').update(JSON.stringify(msg.signedData)).digest();
};

var derToPem = function(der) {
  var pem = '-----BEGIN PUBLIC KEY-----';

  var size = der.length;

  for (var i = 0; i < size; i = i + 64) {
      var end = i + 64 < size ? i + 64 : size;
      pem = pem + '\n' + der.substring(i, end);
  }

  pem = pem + '\n-----END PUBLIC KEY-----';
  return pem;
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

  sign: function(msg, privKey) {
    var pubKey = 'MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEKn3lQ3+/aN6xNd9DSFrYbaPSGOzLMbb1kQZ9lCMtwc6Og4hfCMLhaSbE3sXek8e2fvKrTp8FY1MyCL4qMeVviA==';
    var signer = crypto.createSign(algorithm);
    signer.update(JSON.stringify(msg.signedData));
    var signature = signer.sign(privKey, encoding);
    msg.signature = { signerPubkey: pubKey, signature: signature };
  },

  verify: function(msg) {
    var verifier = crypto.createVerify(algorithm);
    verifier.update(JSON.stringify(msg.signedData));
    var pubkey = derToPem(msg.signature.signerPubkey);
    return verifier.verify(pubkey, msg.signature.signature, encoding);
  }
};
