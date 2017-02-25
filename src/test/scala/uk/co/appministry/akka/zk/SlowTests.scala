package uk.co.appministry.akka.zk

import akka.actor.{Actor, OneForOneStrategy, Props, SupervisorStrategy}
import akka.actor.SupervisorStrategy.Stop
import akka.testkit.TestActorRef
import org.scalatest.time.{Second, Seconds, Span}
import scala.concurrent.duration._

class ConnectionLossTest extends TestBase {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(30, Seconds)), interval = scaled(Span(1, Second)))

  override def afterAll = {}

  "Akka ZK" must {
    "gracefully handle connection loss when ZooKeeper connection lost" in {
      val connectRequest = ZkRequestProtocol.Connect(
        zookeeper.getConnectString,
        sessionTimeout = 10 seconds,
        connectionAttempts = 2)
      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsg( defaultConnectedMsgWait, ZkResponseProtocol.Connected(connectRequest) )
      zookeeper.stop()
      eventually {
        expectMsg(ZkResponseProtocol.Dead(connectRequest))
      }
    }
  }

}

class NoZooKeeperTest extends TestBase {

  override def beforeAll = {
    super.beforeAll
    zookeeper.stop()
  }
  override def afterAll = {}

  "Akka ZK" must {
    "stop the actor if failed to connect to ZooKeeper server" in {
      @volatile var failedToConnect = false
      val watcher = TestActorRef(new Actor {
        override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
          case f: ZkClientConnectionFailedException =>
            failedToConnect = true
            Stop
          case _ => Stop
        }
        def receive = {
          case "run test" =>
            val connectRequest = ZkRequestProtocol.Connect(
              sessionTimeout = 1 second,
              connectionAttempts = 2)
            val actor = this.context.actorOf(Props(new ZkClientActor))
            actor ! connectRequest
            context.watch(actor)
        }
      })
      watcher ! "run test"
      eventually {
        failedToConnect shouldBe true
      }
    }
  }

}
