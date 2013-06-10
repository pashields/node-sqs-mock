delayTime = 30

id = 0
recieptHandle = 0

class SQSQueue
  constructor: ->
    @messages = []
    @hiddenMessages = {}
    @waitingRequests = []

    @VisibilityTimeout = 60
    @CreatedTimestamp = new Date().valueOf()
    @LastModifiedTimestamp = @CreatedTimestamp
    @DelaySeconds = 0
    @ReceiveMessageWaitTimeSeconds = 15

  checkRequests: ->
    # Expected keys VisibilityTimeout, callback
    for request in @waitingRequests
      @getMessage request.VisibilityTimeout, 0, (err, data) =>
        if not err?
          i = @waitingRequests.indexOf request
          @waitingRequests.splice i, 1
          request.callback err, data

  addMessage: (body, delayTime) ->
    msg =
      Body: body
      MessageId: id++

    delayTime = delayTime or @DelaySeconds
    if delayTime > 0
      setTimeout (=> @messages.unshift(msg)), delayTime * 1000
    else
      @messages.unshift(msg)
    process.nextTick =>
      @checkRequests()
    msg

  getMessage: (VisibilityTimeout, WaitTimeSeconds, callback) ->
    msg = @messages.pop()

    if not msg?
      if (WaitTimeSeconds or 0) is 0
        return callback new Error('No message available'), null
      else
        return @waitingRequests.unshift {VisibilityTimeout: VisibilityTimeout, callback: callback}

    msg.ReceiptHandle = recieptHandle++
    @hiddenMessages[msg.RecieptHandle] = msg
    setTimeout(VisibilityTimeout * 1000, (=> @returnMessage(msg.RecieptHandle)))
    callback null, msg

  returnMessage: (RecieptHandle) ->
    msg = @hiddenMessages[RecieptHandle]
    if msg?
      delete @hiddenMessages[RecieptHandle]
      @addMessage msg

  deleteMessage: (RecieptHandle) ->
    msg = @hiddenMessages[RecieptHandle]
    if msg?
      delete @hiddenMessages[RecieptHandle]
    else
      for i in [0..@messages.length]
        candidate = @messages[i]
        if candidate.RecieptHandle is RecieptHandle
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
        @_messageQueues[url] = new SQSQueue()
        callback null, {QueueURL:url}

  deleteMessage: (options, callback) ->
    QueueURL = options.QueueURL
    RecieptHandle = options.RecieptHandle
    queue = @_messageQueues[QueueURL]
    message = queue.deleteMessage RecieptHandle
    
    if message?
      callback null, message
    else
      callback new Error("No message with that Reciept Handle"), null

  getQueueUrl: (options, callback) ->
    QueueName = options.QueueName
    url = @_nameToURL[QueueName]
    if not url?
      callback new Error("Queue with that name does not exist"), null
    else 
      callback null, {QueueURL:url}

  recieveMessage: (options, callback) ->
    QueueURL = options.QueueURL
    MaxNumberOfMessages = options.MaxNumberOfMessages
    VisibilityTimeout = options.VisibilityTimeout
    WaitTimeSeconds = options.WaitTimeSeconds
    @_messageQueues[QueueURL].getMessage VisibilityTimeout, WaitTimeSeconds, callback

  sendMessage: (options, callback) ->
    QueueURL = options.QueueURL
    MessageBody = options.MessageBody
    DelaySeconds = options.DelaySeconds
    msg = @_messageQueues[QueueURL].addMessage MessageBody, DelaySeconds
    callback null, {MessageId: msg.MessageId}

module.exports = SQS