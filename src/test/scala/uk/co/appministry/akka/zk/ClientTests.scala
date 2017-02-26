package uk.co.appministry.akka.zk

import java.util

import akka.actor.Props
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.apache.zookeeper.ZooDefs.OpCode
import org.apache.zookeeper._
import org.apache.zookeeper.data.ACL

import scala.collection.JavaConverters._

class ZooKeeprAvailableTest extends TestBase {

  "Akka ZK" must {

    "connect and disconnect from the ZooKeeper server" in {
      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
      actor ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "correctly handle persistent non-sequential ZooKeeper nodes" in {
      val parent = "/"
      val testNode = "persistent-non-sequential-node-test"
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
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
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
      expectMsgPF() { case ZkResponseProtocol.Data(readDataRequest, data, _) => () }
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

    "correctly handle persistent sequential ZooKeeper nodes" in {
      val parent = "/"
      val testNode = "persistent-sequential-node-test"
      val path = s"$parent$testNode"
      val nodesCreated = new util.ArrayList[String]()

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val createRequest = ZkRequestProtocol.CreatePersistent(path, None, sequential = true)
      val getChildrenRequest = ZkRequestProtocol.GetChildren(parent)

      val actor1 = system.actorOf(Props(new ZkClientActor))
      actor1 ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
      actor1 ! createRequest
      expectMsgPF() { case ZkResponseProtocol.Created(createRequest, result) => {
        nodesCreated.add(result.replace(parent, ""))
        ()
      } }

      val actor2 = system.actorOf(Props(new ZkClientActor))
      actor2 ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
      actor2 ! createRequest
      expectMsgPF() { case ZkResponseProtocol.Created(createRequest, result) => {
        nodesCreated.add(result.replace(parent, ""))
        ()
      } }

      actor1 ! getChildrenRequest
      expectMsg( ZkResponseProtocol.Children(getChildrenRequest, nodesCreated.asScala.toList ++ List("zookeeper")) )

      nodesCreated.asScala.toList.foreach { node =>
        val deleteRequest = ZkRequestProtocol.Delete(s"$parent$node")
        actor1 ! deleteRequest
        expectMsg( ZkResponseProtocol.Deleted(deleteRequest) )
      }

      actor1 ! ZkRequestProtocol.Stop()
      expectNoMsg
      actor2 ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "correctly handle ephemeral non-sequential ZooKeeper nodes" in {
      val parent = "/"
      val testNode = "ephemeral-non-sequential-node-test"
      val path = s"$parent$testNode"
      val data = Some("test data for the node")

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val createRequest = ZkRequestProtocol.CreateEphemeral(path, data)
      val getChildrenRequest = ZkRequestProtocol.GetChildren(parent)
      val countChildrenRequest = ZkRequestProtocol.CountChildren(parent)
      val nodeExistsRequest = ZkRequestProtocol.IsExisting(path)

      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
      actor ! countChildrenRequest
      expectMsg( ZkResponseProtocol.ChildrenCount(countChildrenRequest, 1) )
      actor ! createRequest
      expectMsgPF() { case ZkResponseProtocol.Created(createRequest, result) => () }
      actor ! nodeExistsRequest
      expectMsg( ZkResponseProtocol.Existence(nodeExistsRequest, PathExistenceStatus.Exists) )
      actor ! ZkRequestProtocol.Stop()
      expectNoMsg

      val actorVerifier = system.actorOf(Props(new ZkClientActor))
      actorVerifier ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
      actorVerifier ! nodeExistsRequest
      expectMsg( ZkResponseProtocol.Existence(nodeExistsRequest, PathExistenceStatus.DoesNotExist) )
      actorVerifier ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "correctly handle ephemeral sequential ZooKeeper nodes" in {
      val parent = "/"
      val testNode = "ephemeral-sequential-node-test"
      val path = s"$parent$testNode"
      val nodesCreated = new util.ArrayList[String]()

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val createRequest = ZkRequestProtocol.CreateEphemeral(path, None, sequential = true)
      val getChildrenRequest = ZkRequestProtocol.GetChildren(parent)

      val actor1 = system.actorOf(Props(new ZkClientActor))
      actor1 ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
      actor1 ! createRequest
      expectMsgPF() { case ZkResponseProtocol.Created(createRequest, result) => {
        nodesCreated.add(result.replace(parent, ""))
        ()
      } }

      val actor2 = system.actorOf(Props(new ZkClientActor))
      actor2 ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
      actor2 ! createRequest
      expectMsgPF() { case ZkResponseProtocol.Created(createRequest, result) => {
        nodesCreated.add(result.replace(parent, ""))
        ()
      } }

      actor1 ! getChildrenRequest
      expectMsg( ZkResponseProtocol.Children(getChildrenRequest, nodesCreated.asScala.toList ++ List("zookeeper")) )

      nodesCreated.asScala.toList.foreach { node =>
        val deleteRequest = ZkRequestProtocol.Delete(s"$parent$node")
        actor1 ! deleteRequest
        expectMsg( ZkResponseProtocol.Deleted(deleteRequest) )
      }

      actor1 ! ZkRequestProtocol.Stop()
      expectNoMsg
      actor2 ! ZkRequestProtocol.Stop()
      expectNoMsg

      val actorVerifier = system.actorOf(Props(new ZkClientActor))
      actorVerifier ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
      actorVerifier ! getChildrenRequest
      expectMsg( ZkResponseProtocol.Children(getChildrenRequest, List("zookeeper")) )
      actorVerifier ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "correctly handles ACL without auth info" in {
      val parent = "/"
      val testNode = "acl-test-node"
      val path = s"$parent$testNode"
      val data = Some("test data")
      val acl = List(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE))
      val aclRequest = ZkRequestProtocol.SetAcl(path, acl)
      val dataUpdateRequest = ZkRequestProtocol.WriteData(path, Some("updated data"))

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val createRequest = ZkRequestProtocol.CreateEphemeral(path, None)
      val getChildrenRequest = ZkRequestProtocol.GetChildren(parent)

      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
      actor ! createRequest
      expectMsgPF() { case ZkResponseProtocol.Created(createRequest, result) => () }
      actor ! aclRequest
      expectMsg( ZkResponseProtocol.AclSet(aclRequest) )
      actor ! dataUpdateRequest
      expectMsgPF() { case ZkResponseProtocol.OperationError(dataUpdateRequest, cause) =>
        cause match {
          case e: KeeperException.NoAuthException =>
          case anyOther =>
            fail(s"Expected KeeperException.NoAuthException but got $anyOther")
        }
        ()
      }
      actor ! ZkRequestProtocol.Stop()
      expectNoMsg
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
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
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
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
      actor ! readDataRequest
      expectMsg(ZkResponseProtocol.Data(readDataRequest, None, None))
      actor ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "return OperationException when attempting writing to non-existing node" in {
      val path = s"/write-test"
      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val writeDataRequest = ZkRequestProtocol.WriteData(path, Some("test data"))
      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
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
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }

      actor ! createRequest
      expectMsgPF() { case ZkResponseProtocol.Created(createRequest, result) => () }

      actor ! readDataRequest
      expectMsgPF() { case ZkResponseProtocol.Data(readDataRequest, data, _) => () }

      actor ! writeNoneDataRequest
      expectMsgPF() { case ZkResponseProtocol.Written(writeNoneDataRequest, _) => () }

      actor ! readDataRequest
      expectMsgPF() { case ZkResponseProtocol.Data(_, None, _) => () }

      actor ! writeSomeDataRequest
      expectMsgPF() { case ZkResponseProtocol.Written(writeNoneDataRequest, _) => () }

      actor ! readDataRequest
      expectMsgPF() { case ZkResponseProtocol.Data(readDataRequest, data, _) => () }

      actor ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "correctly handles successful multi" in {
      val parent = "/"
      val nodes = List("multi-test-path1", "multi-test-path2", "multi-test-path3")
      val ops = nodes.map { node => Op.create(s"$parent$node", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) }

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val multiRequest = ZkRequestProtocol.Multi(ops)

      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
      actor ! multiRequest
      expectMsgPF() { case ZkResponseProtocol.MultiResponse(multiRequest, results) => {
        results.filter { opResult => opResult.getType == OpCode.create }.length shouldBe nodes.length
        ()
      } }
      actor ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "correctly handles failing multi" in {
      val parent = "/"
      val nodes = List("multi-test-path1", "multi-test-path2", "with-non-existing-parent/multi-test-path3")
      val ops = nodes.map { node => Op.create(s"$parent$node", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) }

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val multiRequest = ZkRequestProtocol.Multi(ops)

      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
      actor ! multiRequest
      expectMsgPF() { case ZkResponseProtocol.OperationError(multiRequest, cause) => {
        cause match {
          case _: KeeperException.NoNodeException =>
          case anyOther =>
            fail(s"Expected KeeperException.NoNodeException but received $anyOther")
        }
        ()
      } }

      actor ! ZkRequestProtocol.IsExisting(s"$parent${nodes(0)}")
      expectMsgPF() { case ZkResponseProtocol.Existence(_, PathExistenceStatus.DoesNotExist) => () }
      actor ! ZkRequestProtocol.IsExisting(s"$parent${nodes(1)}")
      expectMsgPF() { case ZkResponseProtocol.Existence(_, PathExistenceStatus.DoesNotExist) => () }

      actor ! ZkRequestProtocol.Stop()
      expectNoMsg
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
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }

      actor ! metricsRequest
      expectMsgPF() {
        case ZkResponseProtocol.Metrics(metricsRequest, metrics) =>
          metrics.getOrElse(ZkClientMetricNames.ChildChangePathsObservedCount.name, 0) shouldBe 0
          metrics.getOrElse(ZkClientMetricNames.DataChangePathsObservedCount.name, 0) shouldBe 0
      }

      actor ! subscribeChild
      expectMsg( ZkResponseProtocol.SubscriptionSuccess(subscribeChild) )
      actor ! subscribeData
      expectMsg( ZkResponseProtocol.SubscriptionSuccess(subscribeData) )

      actor ! metricsRequest
      expectMsgPF() {
        case ZkResponseProtocol.Metrics(metricsRequest, metrics) =>
          metrics.getOrElse(ZkClientMetricNames.ChildChangePathsObservedCount.name, 0) shouldBe 1
          metrics.getOrElse(ZkClientMetricNames.DataChangePathsObservedCount.name, 0) shouldBe 1
      }

      actor ! unsubscribeChild
      expectMsg( ZkResponseProtocol.UnsubscriptionSuccess(unsubscribeChild) )
      actor ! unsubscribeData
      expectMsg( ZkResponseProtocol.UnsubscriptionSuccess(unsubscribeData) )

      actor ! metricsRequest
      expectMsgPF() {
        case ZkResponseProtocol.Metrics(metricsRequest, metrics) =>
          metrics.getOrElse(ZkClientMetricNames.ChildChangePathsObservedCount.name, 0) shouldBe 0
          metrics.getOrElse(ZkClientMetricNames.DataChangePathsObservedCount.name, 0) shouldBe 0
      }

      actor ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "forward child change events to a subscriber" in {

      @volatile
      var receivedChangeUpdates = 0

      val path = s"/test-node-forward-child-change"
      val data = Some("with test data")

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val createRequest = ZkRequestProtocol.CreatePersistent(path, data)
      val subscribeRequest = ZkRequestProtocol.SubscribeChildChanges(path)

      implicit val materializer = ActorMaterializer()
      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, publisher) =>

        implicit val materializer = ActorMaterializer()
        Source.fromPublisher[ZkClientStreamProtocol.StreamResponse](publisher).map { message =>
          message match {
            case m: ZkClientStreamProtocol.ChildChange => receivedChangeUpdates = receivedChangeUpdates + 1
            case m: ZkClientStreamProtocol.DataChange  => fail(s"Unexpected data change event: $m")
            case m: ZkClientStreamProtocol.StateChange =>
          }
        }.runWith(Sink.ignore)

        ()
      }

      actor ! subscribeRequest
      expectMsg(ZkResponseProtocol.SubscriptionSuccess(subscribeRequest))

      actor ! createRequest
      expectMsgPF() { case ZkResponseProtocol.Created(createRequest, result) => () }

      eventually {
        receivedChangeUpdates should be > 0
      }

      actor ! ZkRequestProtocol.Stop()
      expectNoMsg

    }

    "forward data change events to a subscriber" in {

      @volatile
      var receivedDataChangeUpdates = 0

      val numberOfUpdates = 10

      val path = s"/test-node-forward-data-change"
      val dataInitial = Some("with test data")
      val dataUpdated = Some("updated data")

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val createRequest = ZkRequestProtocol.CreatePersistent(path, dataInitial)
      val subscribeRequest = ZkRequestProtocol.SubscribeDataChanges(path)

      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest

      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, publisher) =>

        implicit val materializer = ActorMaterializer(
          ActorMaterializerSettings(system).withInputBuffer(
            initialSize = 10,
            maxSize = 64))

        Source.fromPublisher[ZkClientStreamProtocol.StreamResponse](publisher).map { message =>
          message match {
            case m: ZkClientStreamProtocol.ChildChange => fail(s"Unexpected child change event: $m")
            case m: ZkClientStreamProtocol.DataChange  => receivedDataChangeUpdates = receivedDataChangeUpdates + 1
            case m: ZkClientStreamProtocol.StateChange =>
          }
        }.runWith(Sink.ignore)

        ()
      }

      actor ! createRequest
      expectMsgPF() { case ZkResponseProtocol.Created(createRequest, result) => () }

      actor ! subscribeRequest
      expectMsg(ZkResponseProtocol.SubscriptionSuccess(subscribeRequest))

      for (i <- 0 until numberOfUpdates) {
        Thread.sleep(1000)
        actor ! ZkRequestProtocol.WriteData(path, dataUpdated)
        expectMsgPF() { case ZkResponseProtocol.Written(writeNoneDataRequest, _) => () }
      }

      eventually {
        receivedDataChangeUpdates shouldBe numberOfUpdates
      }

      actor ! ZkRequestProtocol.Stop()
      expectNoMsg

    }

    "attempt connecting to a session with password, if these details given" in {
      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString,
        sessionId = Some(System.currentTimeMillis()),
        sessionPassword = Some(Array.empty[Byte]))
      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectNoMsg // the session does not exist on the server so no message should come back
    }

    "create and delete persistent nodes recursively" in {

      val topPath = "/persistent"
      val basePath = s"$topPath/recursive"
      val path = s"$basePath/create/and/delete"

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val createRequest = ZkRequestProtocol.CreatePersistent(path, None, createParents = true)
      val deleteRequest = ZkRequestProtocol.Delete(basePath, recursive = true)

      val existsRequestFull = ZkRequestProtocol.IsExisting(path)
      val existsRequestBase = ZkRequestProtocol.IsExisting(basePath)
      val existsRequestTop = ZkRequestProtocol.IsExisting(topPath)

      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }

      actor ! createRequest
      expectMsg( ZkResponseProtocol.Created(createRequest, path) )

      actor ! existsRequestFull
      expectMsg( ZkResponseProtocol.Existence(existsRequestFull, PathExistenceStatus.Exists) )

      actor ! deleteRequest
      expectMsg( ZkResponseProtocol.Deleted(deleteRequest) )

      actor ! existsRequestBase
      expectMsg( ZkResponseProtocol.Existence(existsRequestBase, PathExistenceStatus.DoesNotExist) )

      actor ! existsRequestTop
      expectMsg( ZkResponseProtocol.Existence(existsRequestTop, PathExistenceStatus.Exists) )

      actor ! ZkRequestProtocol.Stop()
      expectNoMsg

    }

    "create and delete ephemeral nodes recursively" in {

      val topPath = "/ephemeral"
      val basePath = s"$topPath/recursive"
      val path = s"$basePath/create/and/delete"

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val createRequest = ZkRequestProtocol.CreateEphemeral(path, None, createParents = true)
      val deleteRequest = ZkRequestProtocol.Delete(basePath, recursive = true)

      val existsRequestFull = ZkRequestProtocol.IsExisting(path)
      val existsRequestBase = ZkRequestProtocol.IsExisting(basePath)
      val existsRequestTop = ZkRequestProtocol.IsExisting(topPath)

      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }

      actor ! createRequest
      expectMsg( ZkResponseProtocol.Created(createRequest, path) )

      actor ! existsRequestFull
      expectMsg( ZkResponseProtocol.Existence(existsRequestFull, PathExistenceStatus.Exists) )

      actor ! deleteRequest
      expectMsg( ZkResponseProtocol.Deleted(deleteRequest) )

      actor ! existsRequestBase
      expectMsg( ZkResponseProtocol.Existence(existsRequestBase, PathExistenceStatus.DoesNotExist) )

      actor ! existsRequestTop
      expectMsg( ZkResponseProtocol.Existence(existsRequestTop, PathExistenceStatus.Exists) )

      actor ! ZkRequestProtocol.Stop()
      expectNoMsg

    }

    "fail while creating children in ephemeral parents" in {
      val basePath = "/ephemeral-parent"
      val nestedPath = s"$basePath/another-ephemeral/recursive"

      val connectRequest = ZkRequestProtocol.Connect(zookeeper.getConnectString)
      val createRequestParent = ZkRequestProtocol.CreateEphemeral(basePath, None)
      val createRequestNested = ZkRequestProtocol.CreateEphemeral(nestedPath, None, createParents = true)

      val actor = system.actorOf(Props(new ZkClientActor))
      actor ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }

      actor ! createRequestParent
      expectMsg( ZkResponseProtocol.Created(createRequestParent, basePath) )

      actor ! createRequestNested
      expectMsgPF() { case ZkResponseProtocol.OperationError(createRequestNested, _) => ()}

      actor ! ZkRequestProtocol.Stop()
      expectNoMsg

      val nodeExistsRequest = ZkRequestProtocol.IsExisting(basePath)
      val actorVerifier = system.actorOf(Props(new ZkClientActor))
      actorVerifier ! connectRequest
      expectMsgPF(defaultConnectedMsgWait) { case ZkResponseProtocol.Connected(connectRequest, _) => () }
      actorVerifier ! nodeExistsRequest
      expectMsg( ZkResponseProtocol.Existence(nodeExistsRequest, PathExistenceStatus.DoesNotExist) )
      actorVerifier ! ZkRequestProtocol.Stop()
      expectNoMsg
    }

    "handle ACL with auth info" ignore {
      // TODO: find a way to implement this
    }

  }

}
