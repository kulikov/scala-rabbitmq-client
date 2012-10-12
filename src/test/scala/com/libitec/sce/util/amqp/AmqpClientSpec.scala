package com.libitec.sce.util.amqp

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

import com.typesafe.config.ConfigFactory


class AmqpClientSpec extends WordSpec with MustMatchers {

  val configSource = ConfigFactory.parseString("""
    amqp {
      host = "localhost"
      port = 5672
      username = guest
      password = guest
      exchange = test_exchange
      queue = "test.queue"
      virtualHost = /
      heartbeatInterval = 0
    }
                                               """)

  val config = ConfigFactory.load(configSource)

  val amqp = new AmqpClient(new AmqpSettings(config.getConfig("amqp")))

  "AmqpClient" when {

    "subscribe on message" must {
      "receive it" in {
        amqp.subscribe("test.message.key") { (message, env, props) ⇒
          toString(message) must be ("Hello world!")
        }
      }
    }

    "subscribe on undefined routingKey" must {
      "not receive it" in {
        amqp.subscribe("test.undefined.key.123") { (message, env, props) ⇒
          assert(false)
        }
      }
    }

    "publish new message" must {
      "no throw exceptions" in {
        amqp.publish("test.message.key", "Hello world!")
      }
    }
  }

  private def toString(bytes: Array[Byte]) = new String(bytes, "UTF-8")
}
