package com.libitec.sce.util.amqp

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope


/**
 * Mock class for testing amqp client
 *
 * @since 30.07.12 17:22
 * @author kulikov
 */
class AmqpClientTestMock(override val settings: AmqpSettings) extends AmqpClient(settings) {

  var subscribers: Map[String, Set[Handler]] = Map.empty

  /**
   * @since 30.07.12 20:32
   * @author kulikov
   */
  override def subscribe(routingKey: String)(handler: Handler) {
    subscribers.synchronized {
      subscribers += (routingKey → (subscribers.get(routingKey).getOrElse(Set.empty) + handler))
    }
  }

  /**
   * @since 30.07.12 20:32
   * @author kulikov
   */
  override def publish(routingKey: String, message: Array[Byte], props: BasicProperties) {
    subscribers foreach { case (subsKey, handlers) ⇒
      // todo: cache regex for optimization
      val re = ("^\\Q" + subsKey.replaceAll("\\*", """\\E.+\\Q""").replaceAll("\\#", """\\E[^\.]+\\Q""") + "\\E$").r

      if (re.findFirstIn(routingKey).isDefined) {
        handlers foreach(_.apply(message, new Envelope(1, false, settings.Exchange, routingKey), props))
      }
    }
  }
}
