'use strict';

var getHash = function(message) {
  return 'hash';
};

module.exports = { 
  create: function(signedData, publish) {
    var message = {
      signedData: signedData || {},
      isPublished: publish || false, 
    };

    message.signedData.timestamp = message.signedData.timestamp || Date.now;

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
