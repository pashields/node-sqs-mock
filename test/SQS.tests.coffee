assert = require('chai').assert
SQS    = require '../src/SQS'

describe 'Queue Calls:', ->
  sqs = undefined

  beforeEach ->
    sqs = new SQS()

  describe 'when createQueue is called on a non-existant queue', ->
    it 'should create the queue', (done) ->
      options = {QueueName: "my queue"}
      sqs.createQueue options, (err, data) ->
        assert not err?, "Error when creating queue"
        url = data.QueueUrl
        assert url?, "No url returned by queue creation"
        sqs.getQueueUrl options, (err, data) ->
          assert not err?, "Error when getting existing queue url"
          assert.equal url, data.QueueUrl, "getQueueUrl returned different url than create"
          done()

    it 'should create the queue with specified attributes', (done) ->
      options = {QueueName: "my queue", Attributes: {DelaySeconds: 100}}
      sqs.createQueue options, (err, data) ->
        assert not err?, "Error when creating queue with attributes"
        sqs.getQueueAttributes {QueueUrl: data.QueueUrl, Attributes: ["DelaySeconds"]}, (err, data) ->
          assert not err?
          assert.equal data.Attributes.DelaySeconds, 100
          done()

  describe 'when createQueue is called on an existing queue', ->
    options = {QueueName: "my queue"}
    it 'should simply return the queue url', (done) ->
      sqs.createQueue options, (err, data) ->
        url = data.QueueUrl
        sqs.createQueue options, (err, data) ->
          assert not err?, "Calling create on an existing queue fails"
          assert.equal url, data.QueueUrl, "URL returned by create on existing queue is not correct"
          done()

describe 'Message Calls:', ->
  sqs     = undefined
  url     = undefined
  options = undefined

  beforeEach (done) ->
    sqs = new SQS()
    sqs.createQueue {QueueName: "PARTY TIME"}, (err, data) ->
      url = data.QueueUrl
      options =
        QueueUrl: url
        MessageBody: "LEMMY"
      done()

  it 'a message can be successfully sent', (done) ->
    sqs.sendMessage options, (err, data) ->
      assert not err?, "Error when sending message"
      assert data.MessageId?, 'Not message id returned'
      done()

  describe 'a message that has been sent', ->
    it 'can be recieved and deleted', (done) ->
      sqs.sendMessage options, (err, data) ->
        assert not err?, "Error when sending message"
        sqs.recieveMessage options, (err, data) ->
          assert not err?, "Error when recieving message"
          assert.equal data.Body, options.MessageBody, "Recieved body does not match sent"
          assert data.ReceiptHandle?, "No receipt handle on recieved message"
          sqs.deleteMessage {QueueUrl: url, ReceiptHandle: data.ReceiptHandle}, (err, data) ->
            assert not err?
            done()

  describe 'when two messages are sent', ->
    it 'should return them in order', (done) ->
      sqs.sendMessage options, ->
        sqs.sendMessage options, ->
          sqs.recieveMessage options, (err, data) ->
            assert not err?, "Error recieving first message"
            sqs.recieveMessage options, (err, data2) ->
              assert not err?, "Error recieving second message"
              assert data2.MessageId?, "No id on message"
              assert data.MessageId < data2.MessageId, "Messages sent out of order"
              done()

  describe 'after a delayed message is sent', ->
    it 'should be unavailable until it\'s delay completes', (done) ->
      options.DelaySeconds = 2
      sqs.sendMessage options, (err, data) ->
        assert not err?, "Error sending delayed message"
        options.WaitTimeSeconds = 0
        sqs.recieveMessage options, (err, data) ->
          assert err?, "Error calling recieve when no messages are available"
          setTimeout (->
            sqs.recieveMessage options, (err, data) ->
              assert not err?, "Error when recieving delayed message that should be available"
              assert data.Body, options.MessageBody, "Delayed message body does not match sent body"
              done()), 2000

  describe 'a client blocking on an empty queue', ->
    it 'should recieve a message after it\'s addition', (done) ->
      options.WaitTimeSeconds = 20
      sqs.recieveMessage options, (err, data) ->
        assert not err?, "Error in recieving message after block"
        assert.equal data.Body, options.MessageBody
        done()
      sqs.sendMessage options, (err, data) ->
        assert not err?
