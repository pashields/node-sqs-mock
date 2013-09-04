async = require 'async'

delayTime = 30

id = 0
recieptHandle = 0

class SQSQueue
  constructor: (Attributes) ->
    @messages = []
    @hiddenMessages = {}
    @waitingRequests = []
    @delayedMessageCount = 0

    @VisibilityTimeout = '60'
    @CreatedTimestamp = new Date().valueOf()
    @LastModifiedTimestamp = @CreatedTimestamp
    @DelaySeconds = '0'
    @ReceiveMessageWaitTimeSeconds = 1

    @[key] = Attributes[key] for key in Object.keys(Attributes) if Attributes?

  getAttributes: ->
    {
      ApproximateNumberOfMessages: @messages.length,
      ApproximateNumberOfMessagesNotVisible: @hiddenMessages.length,
      VisibilityTimeout: @VisibilityTimeout,
      CreatedTimestamp: @CreatedTimestamp,
      LastModifiedTimestamp: @LastModifiedTimestamp
      ApproximateNumberOfMessagesDelayed: @delayedMessageCount,
      DelaySeconds: @DelaySeconds,
      ReceiveMessageWaitTimeSeconds: @ReceiveMessageWaitTimeSeconds
    }

  checkRequests: ->
    # Expected keys VisibilityTimeout, callback
    async.eachSeries @waitingRequests, (request, cb) =>
      @getMessage request.VisibilityTimeout, 0, (err, data) =>
        if not err?
          i = @waitingRequests.indexOf request
          @waitingRequests.splice i, 1
          request.callback err, data
        cb()

  addMessage: (body, delayTime) ->
    msg =
      Body: body
      MessageId: id++

    delayTime ?= @DelaySeconds
    delayTime = Number(delayTime)
    if delayTime > 0
      @delayedMessageCount++
      setTimeout (=>
        @delayedMessageCount--
        @messages.unshift(msg)
        @checkRequests()), delayTime * 1000
    else
      @messages.unshift(msg)
      process.nextTick =>
        @checkRequests()
    msg

  getMessage: (VisibilityTimeout, WaitTimeSeconds, callback) ->
    msg = @messages.pop()
    WaitTimeSeconds ?= @ReceiveMessageWaitTimeSeconds
    WaitTimeSeconds = Number(WaitTimeSeconds)
    VisibilityTimeout ?= @VisibilityTimeout

    if not msg?
      if WaitTimeSeconds is 0
        return callback new Error('No message available'), null
      else
        request = {VisibilityTimeout, callback}
        setTimeout (=>
          i = @waitingRequests.indexOf request

          return if i < 0

          item = @waitingRequests[i]
          @getMessage item.VisibilityTimeout, 0, item.callback
          @waitingRequests.splice i, 1), WaitTimeSeconds * 1000
        return @waitingRequests.unshift request

    msg.ReceiptHandle = recieptHandle++
    @hiddenMessages[msg.ReceiptHandle] = msg
    setTimeout((=> @returnMessage(msg.ReceiptHandle)), Number(VisibilityTimeout) * 1000)
    callback null, msg

  returnMessage: (ReceiptHandle) ->
    msg = @hiddenMessages[ReceiptHandle]
    if msg?
      delete @hiddenMessages[ReceiptHandle]
      @messages.unshift(msg)
      process.nextTick =>
        @checkRequests()

  deleteMessage: (ReceiptHandle) ->
    msg = @hiddenMessages[ReceiptHandle]
    if msg?
      delete @hiddenMessages[ReceiptHandle]
    else
      for i in [0...@messages.length]
        candidate = @messages[i]
        if candidate.ReceiptHandle is ReceiptHandle
          msg = candidate
          @messages.splice i, 1
          break

class SQS
  constructor: (options) ->
    @_messageQueues = {}
    @_nameToURL = {}

  createQueue: (options, callback) ->
    @getQueueUrl options, (err, data) =>
      if not err?
        callback null, data
      else
        url = options.QueueName
        @_nameToURL[options.QueueName] = url
        @_messageQueues[url] = new SQSQueue(options.Attributes)
        callback null, {QueueUrl:url}

  deleteMessage: (options, callback) ->
    QueueUrl = options.QueueUrl
    ReceiptHandle = options.ReceiptHandle
    queue = @_messageQueues[QueueUrl]
    message = queue.deleteMessage ReceiptHandle

    if message?
      callback null, message
    else
      callback new Error("No message with that Reciept Handle"), null

  getQueueAttributes: (options, callback) ->
    QueueUrl = options.QueueUrl
    Attributes = options.AttributeNames
    attrs = @_messageQueues[QueueUrl].getAttributes()
    reqAttrs = {}
    for attrKey in Attributes
      attrVal = attrs[attrKey]
      if attrVal?
        reqAttrs[attrKey] = attrVal
      else
        return callback new Error("KeyError: #{attrKey} is not a valid Attribute"), null
    callback null, {Attributes: reqAttrs}

  getQueueUrl: (options, callback) ->
    QueueName = options.QueueName
    url = @_nameToURL[QueueName]
    if not url?
      callback new Error("Queue with that name does not exist"), null
    else
      callback null, {QueueUrl:url}

  receiveMessage: (options, callback) ->
    QueueUrl = options.QueueUrl
    MaxNumberOfMessages = options.MaxNumberOfMessages
    VisibilityTimeout = options.VisibilityTimeout
    WaitTimeSeconds = options.WaitTimeSeconds
    @_messageQueues[QueueUrl].getMessage VisibilityTimeout, WaitTimeSeconds, (err, msg) ->
      if err?
        callback err, null
      else
        callback null, {Messages: [msg]}

  sendMessage: (options, callback) ->
    QueueUrl = options.QueueUrl
    MessageBody = options.MessageBody
    DelaySeconds = options.DelaySeconds
    msg = @_messageQueues[QueueUrl].addMessage MessageBody, DelaySeconds
    callback null, {MessageId: msg.MessageId}

module.exports = SQS
