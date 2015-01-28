"use strict"
fs = require("fs")
chai = require("chai")
chaiAsPromised = require("chai-as-promised")
chai.should()
chai.use chaiAsPromised
Message = require("../message.js")

cleanup = ->
  fs.unlink "./test.db"
  return

describe "Database", ->
  db = undefined
  before ->
    cleanup() # After hook fails to execute when errors are thrown
    knex = require("knex")(
      dialect: "sqlite3"
      debug: true
      connection:
        filename: "./test.db"
    )
    db = require("../db.js")(knex)
    return

  after cleanup
  it "should initially have 0 messages", (done) ->
    db.getMessageCount().then (res) ->
      res[0].val.should.equal 0
      done()
      return

    return

  it "should save a message", (done) ->
    message = Message.create(
      type: "rating"
      author: [[
        "email"
        "alice@example.com"
      ]]
      recipient: [[
        "email"
        "bob@example.com"
      ]]
      message: "Positive"
      rating: 1
    )
    Message.sign message, "pubkey"
    db.saveMessage(message).should.eventually.notify done
    return

  hash = undefined
  it "should save another message", (done) ->
    message = Message.create(
      type: "rating"
      author: [[
        "email"
        "charles@example.com"
      ]]
      recipient: [[
        "email"
        "bob@example.com"
      ]]
      message: "Positive"
      rating: -1
    )
    Message.sign message, "pubkey"
    hash = message.hash
    db.saveMessage(message).should.eventually.notify done
    return

  it "should have 2 messages", (done) ->
    db.getMessageCount().then (res) ->
      res[0].val.should.equal 2
      done()
      return

    return

  it "should return message by hash", (done) ->
    db.getMessage(hash).then (res) ->
      res.length.should.equal 1
      done()
      return

    return

  it "should return sent messages", (done) ->
    db.getSent([
      "email"
      "alice@example.com"
    ]).then (res) ->
      res.length.should.equal 1
      done()
      return

    return

  it "should return received messages", (done) ->
    db.getReceived([
      "email"
      "bob@example.com"
    ]).then (res) ->
      res.length.should.equal 2
      done()
      return

    return

  it "should find a saved identifier", (done) ->
    db.identifierSearch("bob").then (res) ->
      res.length.should.equal 1
      res[0].type.should.equal "email"
      res[0].value.should.equal "bob@example.com"
      done()
      return

    return

  it "should save a connection", (done) ->
    message = Message.create(
      type: "confirm_connection"
      author: [[
        "email"
        "alice@example.com"
      ]]
      recipient: [
        [
          "email"
          "bob@example.com"
        ]
        [
          "url"
          "http://www.example.com/bob"
        ]
      ]
    )
    Message.sign message, "pubkey"
    db.saveMessage(message).should.eventually.notify done
    return

  it "should return connecting messages", (done) ->
    db.getConnectingMessages([
      "email"
      "bob@example.com"
    ], [
      "url"
      "http://www.example.com/bob"
    ]).then (res) ->
      res.length.should.equal 1
      done()
      return

    return

  it "should return connected identifiers", (done) ->
    db.getConnectedIdentifiers([
      "email"
      "bob@example.com"
    ]).then (res) ->
      console.log res
      res.length.should.equal 1
      done()
      return

    return

  it "should generate a trust map", (done) ->
    db.generateTrustMap([
      "email"
      "alice@example.com"
    ]).then (res) ->
      res[0].val.should.equal 1
      done()
      return

    return

  return
