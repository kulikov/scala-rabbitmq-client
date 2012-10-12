package com.libitec.sce.util.amqp

import javax.inject.Inject

import com.rabbitmq.client._
import com.rabbitmq.client.AMQP.BasicProperties
import com.typesafe.config.Config


/**
 * Simple amqp-client
 *
 * @since 17.04.12 21:00
 * @author kulikov
 */
class AmqpClient @Inject() (val settings: AmqpSettings) {

  type Handler = (Array[Byte], Envelope, BasicProperties) ⇒ Unit
  type RequestHandler = (Array[Byte], Envelope, BasicProperties) ⇒ Array[Byte]

  private var channelInstance: Channel = null

  private var allSubscriptions = Map.empty[String, Set[Handler]]

  /**
   * Subscribe to messages on routingKey
   * @since 27.06.12 12:35
   * @author kulikov
   */
  def subscribe(routingKey: String)(handler: Handler) {
    val channel = getChannel
    val queueName = settings.Exchange + "." + settings.Queue + "." + routingKey

    channel.queueDeclare(queueName, false, false, false, null) // not durable, not exclusive, not auto delete queue
    channel.queueBind(queueName, settings.Exchange, routingKey)

    val consumer = new DefaultConsumer(channel) {
      override def handleDelivery(tag: String, env: Envelope, props: BasicProperties, body: Array[Byte]) {
        try {
          handler(body, env, props)
          channel.basicAck(env.getDeliveryTag, false)
        } catch {
          case e: Exception ⇒
            println(e.getMessage);
            e.printStackTrace() // don't ack message if something wrong
        }
      }
    }

    channel.basicConsume(queueName, consumer)

    // collect all subscriptions for resubscribe them after reconnect
    allSubscriptions.synchronized {
      allSubscriptions += (routingKey → (allSubscriptions.get(routingKey).getOrElse(Set.empty) + handler))
    }
  }

  /**
   * Subscribe to request and auto response after handling
   * Request it is special message with replayTo routing. Handler result publish to replayTo routing key
   * @since 27.06.12 12:35
   * @author kulikov
   */
  def subscribeRequest(routingKey: String)(handler: RequestHandler) {
    subscribe(routingKey) { (message, env, props) ⇒
      val response = handler(message, env, props)

      if (props.getReplyTo != null && props.getReplyTo != "") {
        publish(props.getReplyTo, response)
      }
    }
  }

  /**
   * Publish message
   * @since 27.06.12 12:33
   * @author kulikov
   */
  def publish(routingKey: String, message: String) { publish(routingKey, message, null) }
  def publish(routingKey: String, message: String, props: BasicProperties) { publish(routingKey, message.getBytes("UTF-8"), props) }
  def publish(routingKey: String, message: Array[Byte]) { publish(routingKey: String, message: Array[Byte], null) }

  def publish(routingKey: String, message: Array[Byte], props: BasicProperties) {
    getChannel.basicPublish(settings.Exchange, routingKey, props, message)
  }

  /**
   * Request
   * Publish message with uniq replayTo key and subscribe to response on replayTo routing key
   * @since 28.04.12 13:38
   * @author kulikov
   */
  def request(routingKey: String, message: String)(responseHandler: Handler) { request(routingKey, message.getBytes("UTF-8"))(responseHandler) }

  def request(routingKey: String, message: Array[Byte])(responseHandler: Handler) {

    // uniq routing key for replayTo
    val uniqRoutingKey = "response.%s.%s" format(routingKey, java.util.UUID.randomUUID)

    subscribe(uniqRoutingKey) { (msg, env, props) ⇒
      allSubscriptions.synchronized {
        allSubscriptions -= uniqRoutingKey // cleanup
      }

      responseHandler(msg, env, props)
    }

    val props = new BasicProperties.Builder()
      .replyTo(uniqRoutingKey)
      .build()

    publish(routingKey, message, props)
  }

  /**
   * Connect to rabbitmq, register channel
   */
  private def createNewChannel: Channel = {
    val connection = createNewConnection

    connection.addShutdownListener(new ShutdownListener {
      def shutdownCompleted(e: ShutdownSignalException) {
        if (e.isHardError && !e.isInitiatedByApplication) {
          tryReconnect()
        }
      }
    })

    val newChannel = connection.createChannel()
    newChannel.exchangeDeclare(settings.Exchange, "topic", true) // durable
    newChannel
  }

  /**
   * Create new connection to amqp server
   * Try reconnect if error
   */
  private def createNewConnection: Connection = {

    val factory = new ConnectionFactory()
    factory.setHost(settings.Host)
    factory.setPort(settings.Port)
    factory.setUsername(settings.Username)
    factory.setRequestedHeartbeat(settings.HeartbeatInterval)
    factory.setPassword(settings.Password)
    factory.setVirtualHost(settings.VirtualHost)
    factory.setConnectionTimeout(5000) // 5 seconds

    // trying to connect
    try {
      factory.newConnection()
    } catch {
      case e: Exception ⇒
        // sleep 3 seconds and try reconnect
        Thread.sleep(3000)
        createNewConnection
    }
  }

  /**
   * Reconnect to server
   */
  private def tryReconnect() {
    Thread.sleep(1000)

    val newChannel = createNewChannel

    channelInstance.synchronized {
      channelInstance = newChannel
    }

    // get all exists subscriptions and reset list for avoiding duplicate
    val allSubs = allSubscriptions.synchronized {
      val _allSubs = allSubscriptions
      allSubscriptions = Map.empty
      _allSubs
    }

    // resubscribe all exists handlers
    for ((rtKey, handlers) ← allSubs; handler ← handlers) {
      subscribe(rtKey)(handler)
    }
  }

  /**
   * Return channel instance
   * @since 30.07.12 19:02
   * @author kulikov
   */
  private def getChannel: Channel = {
    if (channelInstance == null) {
      this.synchronized {
        if (channelInstance == null) {
          channelInstance = createNewChannel
        }
      }
    }
    channelInstance
  }

}


/**
 * RabbitMQ settings
 */
class AmqpSettings(config: Config) {
  val Host = config.getString("host")
  val Port = config.getInt("port")
  val Username = config.getString("username")
  val Password = config.getString("password")
  val Exchange = config.getString("exchange")
  val Queue = config.getString("queue")
  val VirtualHost = config.getString("virtualHost")
  val HeartbeatInterval = config.getInt("heartbeatInterval")
}
