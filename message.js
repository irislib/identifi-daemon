/*jshint unused: false */
'use strict';
var crypto = require('crypto');
var jws = require('jws');

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

    return msg;
  },

  sign: function(msg, privKey, keyID) {
    msg.jwsHeader = { alg: 'ES256', kid: keyID };
    msg.jws = jws.sign({
      header: msg.jwsHeader,
      payload: msg.signedData,
      privateKey: privKey
    });
    msg.hash = getHash(msg).toString(encoding);
    return msg.jws;
  },

  decode: function(msg) {
    if (!msg.signedData) {
      var d = jws.decode(msg.jws);
      msg.signedData = d.payload;
      msg.jwsHeader = d.header;
      msg.hash = getHash(msg).toString(encoding);
    }
    return msg.jwsData;
  },

  verify: function(msg, pubKey) {
    this.decode(msg);
    return jws.verify(msg.jws, msg.jwsHeader.alg, pubKey); 
  },

  parse: function(jws) {
    var msg = { jws: jws };
    this.decode(msg);
    return msg;
  },

  isPositive: function(msg) {
    var d = msg.signedData;
    return d.rating > (d.maxRating + d.minRating) / 2;
  }
};
