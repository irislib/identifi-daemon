'use strict';

var knex = require('knex')({
  dialect: 'sqlite3',
  connection: {
    filename: './data.db'
  }
});
var db = require('./db.js')(knex);

var identifi = {
  run: function() {
    db.getMessageCount().map(function(res) {
      console.log(res.val + ' messages in db');
    });
  }
};

module.exports = identifi;

identifi.run();
