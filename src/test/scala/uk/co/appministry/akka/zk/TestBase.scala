package uk.co.appministry.akka.zk

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import uk.co.appministry.akka.zk.utils.FreePort

import scala.util.Try

class TestBase extends  TestKit(ActorSystem("testing")) with ImplicitSender
  with WordSpecLike
  with BeforeAndAfterAll
  with Matchers
  with Eventually {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(20, Seconds)), interval = scaled(Span(100, Millis)))

  var zookeeper: TestingServer = _

  override def beforeAll {
    super.beforeAll()
    ConfigFactory.load().resolve()
    zookeeper = new TestingServer(FreePort.getFreePort)
    zookeeper.start()
  }

  override def afterAll {
    super.afterAll()
    zookeeper.stop()
  }

}
