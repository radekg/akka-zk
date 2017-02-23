package uk.co.appministry.akka.zk

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, OneForOneStrategy, Props, SupervisorStrategy}
import akka.testkit.TestActorRef

class ZooKeeprAvailableTest extends TestBase {

  "Akka ZK" must {

    "connect and disconnect from the ZooKeeper server" in {
      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsg( ZkResponseProtocol.Connected(connectRequest) )
      expectMsgPF() { case ZkResponseProtocol.StateChange(_) => () }
      actor ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "correctly handle persistent non-sequential ZooKeeper nodes" in {
      val parent = "/"
      val testNode = "hello-test-world"
      val path = s"$parent$testNode"
      val data = Some("test data for the node")

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val createRequest = ZkRequestProtocol.CreatePersistent(path, data)
      val getChildrenRequest = ZkRequestProtocol.GetChildren(parent)
      val countChildrenRequest = ZkRequestProtocol.CountChildren(parent)
      val nodeExistsRequest = ZkRequestProtocol.IsExisting(path)
      val readDataRequest = ZkRequestProtocol.ReadData(path)
      val creationTimeRequest = ZkRequestProtocol.CreatedWhen(path)
      val deleteRequest = ZkRequestProtocol.Delete(path)

      val actor = system.actorOf(Props(new ZkClientActor))
      // connect:
      actor ! connectRequest
      expectMsg( ZkResponseProtocol.Connected(connectRequest) )
      expectMsgPF() { case ZkResponseProtocol.StateChange(_) => () }
      // ZooKeeper should be clean:
      actor ! countChildrenRequest
      expectMsg( ZkResponseProtocol.ChildrenCount(countChildrenRequest, 1) )
      // create a node:
      actor ! createRequest
      expectMsgPF() { case ZkResponseProtocol.Created(createRequest, result) => () }
      // number of children should be increased:
      actor ! countChildrenRequest
      expectMsg( ZkResponseProtocol.ChildrenCount(countChildrenRequest, 2) )
      // the node should be found under the parent
      actor ! getChildrenRequest
      expectMsg( ZkResponseProtocol.Children(getChildrenRequest, List(testNode, "zookeeper")) )
      // the node exists:
      actor ! nodeExistsRequest
      expectMsg( ZkResponseProtocol.Existence(nodeExistsRequest, PathExistenceStatus.Exists) )
      // read the data
      actor ! readDataRequest
      expectMsg( ZkResponseProtocol.Data(readDataRequest, data) )
      // creation time:
      actor ! creationTimeRequest
      expectMsgPF() { case ZkResponseProtocol.CreatedAt(creationTimeRequest, _) => () }
      // delete node
      actor ! deleteRequest
      expectMsg( ZkResponseProtocol.Deleted(deleteRequest) )
      // check that it does not exist anymore
      actor ! nodeExistsRequest
      expectMsg( ZkResponseProtocol.Existence(nodeExistsRequest, PathExistenceStatus.DoesNotExist) )
      // stop
      actor ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "correctly handles ACL without auth info" in {
      // TODO: implement
    }

    "correctly handles ACL with auth info" in {
      // TODO: implement
    }

    "correctly handle persistent sequential ZooKeeper nodes" in {
      // TODO: implement
    }

    "correctly handle ephemeral non-sequential ZooKeeper nodes" in {
      // TODO: implement
    }

    "correctly handle ephemeral sequential ZooKeeper nodes" in {
      // TODO: implement
    }

    "reply to SASL status request" in {
      val actor = system.actorOf(Props(new ZkClientActor))
      val request = ZkRequestProtocol.IsSaslEnabled()
      actor ! request
      expectMsg( ZkResponseProtocol.Sasl(request, SaslStatus.Disabled) )
    }

    "return OperationException when attempting reading data from non-existing node" in {
      val path = s"/write-test"
      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val readDataRequest = ZkRequestProtocol.ReadData(path)
      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsg( ZkResponseProtocol.Connected(connectRequest) )
      expectMsgPF() { case ZkResponseProtocol.StateChange(_) => () }
      actor ! readDataRequest
      expectMsgPF() { case ZkResponseProtocol.OperationError(readDataRequest, _) => () }
      actor ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "return None when attempting reading data from non-existing node, if asked" in {
      val path = s"/write-test"
      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val readDataRequest = ZkRequestProtocol.ReadData(path, noneIfNoPath = true)
      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsg( ZkResponseProtocol.Connected(connectRequest) )
      expectMsgPF() { case ZkResponseProtocol.StateChange(_) => () }
      actor ! readDataRequest
      expectMsg(ZkResponseProtocol.Data(readDataRequest, None))
      actor ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "return OperationException when attempting writing to None existing node" in {
      val path = s"/write-test"
      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val writeDataRequest = ZkRequestProtocol.WriteData(path, Some("test data"))
      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsg( ZkResponseProtocol.Connected(connectRequest) )
      expectMsgPF() { case ZkResponseProtocol.StateChange(_) => () }
      actor ! writeDataRequest
      expectMsgPF() { case ZkResponseProtocol.OperationError(writeDataRequest, _) => () }
      actor ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "correctly handle writes to ZooKeeper nodes" in {

      val path = s"/write-test"
      val data = Some("test data for the node")

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val createRequest = ZkRequestProtocol.CreatePersistent(path, data)
      val readDataRequest = ZkRequestProtocol.ReadData(path)
      val writeNoneDataRequest = ZkRequestProtocol.WriteData(path, None)
      val writeSomeDataRequest = ZkRequestProtocol.WriteData(path, data)

      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsg( ZkResponseProtocol.Connected(connectRequest) )
      expectMsgPF() { case ZkResponseProtocol.StateChange(_) => () }

      actor ! createRequest
      expectMsgPF() { case ZkResponseProtocol.Created(createRequest, result) => () }

      actor ! readDataRequest
      expectMsg( ZkResponseProtocol.Data(readDataRequest, data) )

      actor ! writeNoneDataRequest
      expectMsgPF() { case ZkResponseProtocol.Written(writeNoneDataRequest, _) => () }

      actor ! readDataRequest
      expectMsg( ZkResponseProtocol.Data(readDataRequest, None) )

      actor ! writeSomeDataRequest
      expectMsgPF() { case ZkResponseProtocol.Written(writeNoneDataRequest, _) => () }

      actor ! readDataRequest
      expectMsg( ZkResponseProtocol.Data(readDataRequest, data) )

      actor ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "correctly handles multi" in {
      // TODO: implement
    }

    "subscribe and unsubscribe for child change events" in {
      val path = s"/write-test"

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val subscribeChild = ZkRequestProtocol.SubscribeChildChanges(path)
      val subscribeData = ZkRequestProtocol.SubscribeDataChanges(path)
      val unsubscribeChild = ZkRequestProtocol.UnsubscribeChildChanges(path)
      val unsubscribeData = ZkRequestProtocol.UnsubscribeDataChanges(path)
      val metricsRequest = ZkRequestProtocol.Metrics()

      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsg( ZkResponseProtocol.Connected(connectRequest) )
      expectMsgPF() { case ZkResponseProtocol.StateChange(_) => () }

      actor ! metricsRequest
      expectMsgPF() {
        case ZkResponseProtocol.Metrics(metricsRequest, metrics) =>
          metrics.getOrElse(ZkClientMetricNames.ChildChangeSubscribersCount.name, 0) shouldBe 0
          metrics.getOrElse(ZkClientMetricNames.DataChangeSubscribersCount.name, 0) shouldBe 0
      }

      actor ! subscribeChild
      expectMsg( ZkResponseProtocol.SubscriptionSuccess(subscribeChild) )
      actor ! subscribeData
      expectMsg( ZkResponseProtocol.SubscriptionSuccess(subscribeData) )

      actor ! metricsRequest
      expectMsgPF() {
        case ZkResponseProtocol.Metrics(metricsRequest, metrics) =>
          metrics.getOrElse(ZkClientMetricNames.ChildChangeSubscribersCount.name, 0) shouldBe 1
          metrics.getOrElse(ZkClientMetricNames.DataChangeSubscribersCount.name, 0) shouldBe 1
      }

      actor ! unsubscribeChild
      expectMsg( ZkResponseProtocol.UnsubscriptionSuccess(unsubscribeChild) )
      actor ! unsubscribeData
      expectMsg( ZkResponseProtocol.UnsubscriptionSuccess(unsubscribeData) )

      actor ! metricsRequest
      expectMsgPF() {
        case ZkResponseProtocol.Metrics(metricsRequest, metrics) =>
          metrics.getOrElse(ZkClientMetricNames.ChildChangeSubscribersCount.name, 0) shouldBe 0
          metrics.getOrElse(ZkClientMetricNames.DataChangeSubscribersCount.name, 0) shouldBe 0
      }

      actor ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "forward data change events to a subscriber" in {

      var watcherSubscribed = false
      @volatile
      var receivedDataChangeUpdates = 0

      val numberOfUpdates = 2

      val path = s"/test-node"
      val dataInitial = Some("with test data")
      val dataUpdated = Some("updated data")

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val createRequest = ZkRequestProtocol.CreatePersistent(path, dataInitial)

      val actor = system.actorOf(Props(new ZkClientActor))

      val watcher = TestActorRef(new Actor {
        def receive = {
          case "subscribe" =>
            actor ! ZkRequestProtocol.SubscribeDataChanges(path)
          case ZkResponseProtocol.SubscriptionSuccess(_) =>
            watcherSubscribed = true
          case ZkResponseProtocol.DataChange(_) =>
            // FIXME: this isn't really logical, there should be no need to resubsribe unless there's a specific
            // reason for it.
            // However, if there's a reason, why are we allowed to register multiple subscribers?
            actor ! ZkRequestProtocol.SubscribeDataChanges(path)
            receivedDataChangeUpdates = receivedDataChangeUpdates + 1
          case anyOther =>
            fail(s"Unexpected message $anyOther received.")
        }
      })

      actor ! connectRequest
      expectMsg( ZkResponseProtocol.Connected(connectRequest) )
      expectMsgPF() { case ZkResponseProtocol.StateChange(_) => () }

      actor ! createRequest
      expectMsgPF() { case ZkResponseProtocol.Created(createRequest, result) => () }

      watcher ! "subscribe"
      eventually {
        watcherSubscribed shouldBe true
      }

      for (i <- 0 until numberOfUpdates) {
        actor ! ZkRequestProtocol.WriteData(path, dataUpdated)
        expectMsgPF() { case ZkResponseProtocol.Written(writeNoneDataRequest, _) => () }
      }

      eventually {
        receivedDataChangeUpdates shouldBe numberOfUpdates
      }

      actor ! ZkRequestProtocol.Stop()
      expectNoMsg

    }

    "forward child change events to a subscriber" in {

      var watcherSubscribed = false
      @volatile
      var receivedChangeUpdates = 0

      val path = s"/test-node"
      val data = Some("with test data")

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val createRequest = ZkRequestProtocol.CreatePersistent(path, data)

      val actor = system.actorOf(Props(new ZkClientActor))

      val watcher = TestActorRef(new Actor {
        def receive = {
          case "subscribe" =>
            actor ! ZkRequestProtocol.SubscribeChildChanges(path)
          case ZkResponseProtocol.SubscriptionSuccess(_) =>
            watcherSubscribed = true
          case ZkResponseProtocol.ChildChange(_) =>
            receivedChangeUpdates = receivedChangeUpdates + 1
          case anyOther =>
            fail(s"Unexpected message $anyOther received.")
        }
      })

      actor ! connectRequest
      expectMsg( ZkResponseProtocol.Connected(connectRequest) )
      expectMsgPF() { case ZkResponseProtocol.StateChange(_) => () }

      watcher ! "subscribe"
      eventually {
        watcherSubscribed shouldBe true
      }

      actor ! createRequest
      expectMsgPF() { case ZkResponseProtocol.Created(createRequest, result) => () }

      eventually {
        receivedChangeUpdates should be > 0
      }

      actor ! ZkRequestProtocol.Stop()
      expectNoMsg

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
    "connect and disconnect from the ZooKeeper server" in {
      import scala.concurrent.duration._
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
            val connectRequest = ZkRequestProtocol.Connect(connectionTimeout = 1 second, connectionAttempts = 2)
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