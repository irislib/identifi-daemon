/*jshint unused: false */
'use strict';
var CryptoJS = require('crypto-js');

var getHash = function(message) {
  return CryptoJS.SHA256(JSON.stringify(message.signedData)).toString(CryptoJS.enc.Base64);
};

module.exports = { 
  create: function(signedData, publish) {
    var message = {
      signedData: signedData,
      isPublished: publish || false, 
    };

    message.signedData.timestamp = message.signedData.timestamp || Date.now();

    return message;
  },
  parse: function(data) {
    return false;
  },
  sign: function(message, key) {
    message.signature = { signerPubkey: '', signature: '' };
    message.hash = getHash(message);
    return message;
  }
};
