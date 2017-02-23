package uk.co.appministry.akka.zk

import java.io.File
import java.util
import java.util.ArrayList
import java.util.{List => JList}
import java.util.concurrent.ConcurrentHashMap
import javax.security.auth.login.Configuration

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.codahale.metrics.MetricRegistry
import org.I0Itec.zkclient.ZkConnection
import org.I0Itec.zkclient.serialize.{SerializableSerializer, ZkSerializer}
import org.apache.zookeeper._
import org.apache.zookeeper.data.{ACL, Stat}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.{Failure, Success, Try}

/**
  * Represents a connectivity issue.
  * Thrown if the ZooKeeper client is unable to correct to the server.
  * This error will terminate the instance of the actor.
  */
case class ZkClientConnectionFailedException() extends Exception()

/**
  * Represents an internal ZooKeeper client error.
  * Such errors are not recoverable as they indicate state corruption.
  * @param message Human readable description of the problem.
  */
case class ZkClientInvalidStateException(val message: String) extends Exception(message)

/**
  * SASL configuration file related exception.
  * @param message Human readable description of the problem.
  */
case class ZkSaslConfigurationFileException(val message: String) extends Exception(message)

/**
  * ACL entry.
  * @param acl access control list
  * @param stat node stat
  */
case class AclEntry(val acl: List[ACL], val stat: Stat)

/**
  * ZooKeeper node existence status.
  */
object PathExistenceStatus {
  sealed trait Status

  /**
    * Represents existing node.
    */
  final case object Exists extends Status

  /**
    * Represents non-existing node.
    */
  final case object DoesNotExist extends Status
}

object SaslProperties {
  final val JavaLoginConfigParam = "java.security.auth.login.config"
  final val ZkSaslCLlient = "zookeeper.sasl.client"
  final val ZkLoginContextNameKey = "zookeeper.sasl.clientconfig"
}

/**
  * SASL status.
  */
object SaslStatus {
  sealed trait Status
  sealed trait DisabledStatus extends Status

  /**
    * SASL enabled.
    */
  final case object Enabled extends Status

  /**
    * SASL disabled.
    */
  final case object Disabled extends DisabledStatus

  /**
    * SASL explicitly disabled.
    */
  final case object DisabledExplicitly extends DisabledStatus
}

/**
  * ZooKeeper client messages.
  */
object ZkClientMessages {
  val ConnectRequestMissing = "Invalid state. Connect request is missing."
  val ConnectRequestorMissing = "Invalid state. Connect requestor is missing."
  val ConnectionMissing = "Invalid state. Connection is missing."
}

/**
  * ZooKeeper client default values' settings.
  */
object ZkClientProtocolDefaults {
  val ConnectionString = "localhost:2181"
  val ConnectionTimeout = 30 seconds
  val ConnectionAttempts = 2
  val SessionTimeout = 30 seconds
}

object ZkClientMetricNames {
  sealed abstract class MetricName(val name: String)
  case object ChildChangeSubscribersCount extends MetricName("child-change-subscribers-count")
  case object DataChangeSubscribersCount extends MetricName("data-change-subscribers-count")
}

/**
  * ZooKeeper client request protocol.
  */
object ZkRequestProtocol {

  /**
    * ZooKeeper client request.
    */
  sealed trait Request

  /**
    * ZooKeeper client node creation request.
    */
  sealed trait CreateRequest extends Request

  /**
    * ZooKeeper client subscription request.
    */
  sealed trait SubscribeRequest extends Request

  /**
    * ZooKeeper client unsubscription request.
    */
  sealed trait UnsubscribeRequest extends Request

  /**
    *
    * @param scheme
    * @param authInfo
    */
  final case class AddAuthInfo(val scheme: String, val authInfo: Array[Byte]) extends Request

  /**
    *
    * @param connectionString
    * @param connectionTimeout
    * @param connectionAttempts
    * @param sessionTimeout
    */
  final case class Connect(val connectionString: String = ZkClientProtocolDefaults.ConnectionString,
                           val connectionTimeout: FiniteDuration = ZkClientProtocolDefaults.ConnectionTimeout,
                           val connectionAttempts: Int = ZkClientProtocolDefaults.ConnectionAttempts,
                           val sessionTimeout: FiniteDuration = ZkClientProtocolDefaults.SessionTimeout) extends Request

  /**
    *
    * @param path
    */
  final case class CountChildren(val path: String) extends Request

  /**
    *
    * @param path
    * @param data
    * @param acls
    * @param sequential
    */
  final case class CreateEphemeral(val path: String,
                                   val data: Option[Any] = None,
                                   val acls: List[ACL] = ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala.toList,
                                   val sequential: Boolean = false) extends CreateRequest

  /**
    *
    * @param path
    * @param data
    * @param acls
    * @param sequential
    */
  final case class CreatePersistent(val path: String,
                                    val data: Option[Any] = None,
                                    val acls: List[ACL] = ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala.toList,
                                    val sequential: Boolean = false) extends CreateRequest

  /**
    *
    * @param path
    */
  final case class CreatedWhen(val path: String) extends Request

  /**
    *
    * @param path
    * @param version
    */
  final case class Delete(val path: String, val version: Int = -1) extends Request

  /**
    *
    * @param path
    */
  final case class GetAcl(val path: String) extends Request

  /**
    *
    * @param path
    * @param watch
    */
  final case class GetChildren(val path: String, val watch: Option[Boolean] = None) extends Request

  /**
    *
    * @param path
    */
  final case class IsExisting(val path: String) extends Request

  final case class IsSaslEnabled() extends Request

  /**
    *
    * @param path
    * @param stat
    * @param watch
    * @param noneIfNoPath
    */
  final case class ReadData(val path: String,
                            val stat: Option[Stat] = None,
                            val watch: Option[Boolean] = None,
                            val noneIfNoPath: Boolean = false) extends Request

  /**
    *
    */
  final case class Metrics() extends Request

  /**
    *
    * @param ops
    */
  final case class Multi(val ops: List[Op]) extends Request

  /**
    *
    * @param path
    * @param acl
    */
  final case class SetAcl(val path: String, val acl: List[ACL]) extends Request

  /**
    * Set the read / write serializer for this client.
    * @param serializer the serializer
    */
  final case class SetSerializer(val serializer: ZkSerializer) extends Request

  /**
    * Stop the ZooKeeper client. Disconnect from ZooKeeper, if necessary.
    */
  final case class Stop()

  /**
    *
    * @param path
    */
  final case class SubscribeChildChanges(val path: String) extends SubscribeRequest

  /**
    *
    * @param path
    */
  final case class SubscribeDataChanges(val path: String) extends SubscribeRequest

  /**
    *
    * @param path
    */
  final case class UnsubscribeChildChanges(val path: String) extends UnsubscribeRequest

  /**
    *
    * @param path
    */
  final case class UnsubscribeDataChanges(val path: String) extends UnsubscribeRequest

  /**
    *
    * @param path
    * @param data
    * @param expectedVersion
    */
  final case class WriteData(val path: String,
                             val data: Option[Any],
                             val expectedVersion: Int = -1) extends Request

}

/**
  * ZooKeeper client response protocol.
  */
object ZkResponseProtocol {

  /**
    * ZooKeeper client response.
    */
  sealed trait Response

  /**
    *
    * @param request original request
    * @param entry ACL data entry
    */
  final case class AclData(val request: ZkRequestProtocol.GetAcl, val entry: AclEntry) extends Response

  /**
    *
    * @param request original request
    */
  final case class AclSet(val request: ZkRequestProtocol.SetAcl) extends Response

  /**
    *
    * @param request original request
    */
  final case class AuthInfoAdded(val request: ZkRequestProtocol.AddAuthInfo) extends Response

  /**
    *
    */
  final case class AwaitingConnection() extends Response

  /**
    *
    * @param event
    */
  final case class ChildChange(val event: WatchedEvent) extends Response

  /**
    *
    * @param request original request
    * @param result list of node's children
    */
  final case class Children(val request: ZkRequestProtocol.GetChildren, val result: List[String]) extends Response

  /**
    *
    * @param request original request
    * @param result number of children nodes
    */
  final case class ChildrenCount(val request: ZkRequestProtocol.CountChildren, val result: Int) extends Response

  /**
    *
    * @param request original request
    */
  final case class Connected(val request: ZkRequestProtocol.Connect) extends Response

  /**
    *
    * @param request original request
    * @param result the actual path of the created node
    */
  final case class Created(val request: ZkRequestProtocol.CreateRequest, val result: String) extends Response

  /**
    *
    * @param request original request
    * @param timestamp
    */
  final case class CreatedAt(val request: ZkRequestProtocol.CreatedWhen, val timestamp: Long) extends Response

  /**
    *
    * @param request original request
    * @param data
    */
  final case class Data(val request: ZkRequestProtocol.ReadData, val data: Option[Any]) extends Response

  /**
    *
    * @param event
    */
  final case class DataChange(val event: WatchedEvent) extends Response

  /**
    *
    */
  final case class Disconnected() extends Response

  /**
    *
    * @param request original request
    */
  final case class Deleted(val request: ZkRequestProtocol.Delete) extends Response

  /**
    *
    * @param request original request
    * @param status
    */
  final case class Existence(val request: ZkRequestProtocol.IsExisting, val status: PathExistenceStatus.Status) extends Response

  /**
    *
    * @param request original request
    * @param metrics current metrics snapshot
    */
  final case class Metrics(val request: ZkRequestProtocol.Metrics, val metrics: Map[String, Long]) extends Response

  /**
    *
    * @param request original request
    * @param results
    */
  final case class MultiResponse(val request: ZkRequestProtocol.Multi, val results: List[OpResult]) extends Response

  /**
    * Wraps any potential ZooKeeper exception encountered while executing an operation.
    *
    * @param request request for which the exception occured
    * @param reason  original exception
    */
  final case class OperationError(val request: ZkRequestProtocol.Request, val reason: Throwable) extends Response

  /**
    *
    * @param request
    * @param status
    */
  final case class Sasl(val request: ZkRequestProtocol.IsSaslEnabled, val status: SaslStatus.Status) extends Response

  /**
    *
    * @param event
    */
  final case class StateChange(val event: WatchedEvent) extends Response

  /**
    *
    * @param request
    */
  final case class SubscriptionSuccess(val request: ZkRequestProtocol.SubscribeRequest) extends Response

  /**
    *
    * @param request
    */
  final case class UnsubscriptionSuccess(val request: ZkRequestProtocol.UnsubscribeRequest) extends Response

  /**
    *
    * @param request original request
    * @param stat
    */
  final case class Written(val request: ZkRequestProtocol.WriteData, val stat: Stat) extends Response

}

/**
  * Internal protocol.
  */
object ZkInternalProtocol {

  /**
    * Internal protocol message.
    */
  private[zk] sealed trait Internal

  /**
    * Client watcher connection report.
    */
  private[zk] final case class ZkConnectionSuccessful()

  /**
    * ZooKeeper client failed to connect within the deadline.
    */
  private[zk] final case class ZkInitialConnectionDeadline()

  /**
    * A ZooKeeper state change event is ready for processing.
    * @param event the state event
    */
  private[zk] final case class ZkProcessStateChange(val event: WatchedEvent)

  /**
    * A ZooKeeper child change event is ready for processing.
    * @param event the data or child event
    */
  private[zk] final case class ZkProcessChildChange(val event: WatchedEvent)

  /**
    * A ZooKeeper node data change event is ready for processing.
    * @param event the data or child event
    */
  private[zk] final case class ZkProcessDataChange(val event: WatchedEvent)
}

/**
  * ZooKeeper client state representation.
  * @param currentAttempt current connection attempt
  * @param connectRequest maximum number of connection attempts
  * @param requestor [[akka.actor.ActorRef]] of the actor requesting the connection
  * @param connection underlaying ZooKeeper connection
  * @param serializer serializer used for reading and writing the data
  */
case class ZkClientState(val currentAttempt: Int,
                         val connectRequest: Option[ZkRequestProtocol.Connect],
                         val requestor: Option[ActorRef],
                         val connection: Option[ZkConnection] = None,
                         val serializer: ZkSerializer = new SerializableSerializer,
                         val dataChangeSubscriptions: ConcurrentHashMap[String, JList[ActorRef]] = new ConcurrentHashMap[String, JList[ActorRef]](),
                         val childChangeSubscriptions: ConcurrentHashMap[String, JList[ActorRef]] = new ConcurrentHashMap[String, JList[ActorRef]]())

/**
  * Akka ZooKeeper client.
  *
  * TODO: usage...
  */
class ZkClientActor extends Actor with ActorLogging with ZkClientWatcher {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  def receive = connect(ZkClientState(1, None, None))

  def connect(state: ZkClientState): Receive = {
    case req @ ZkRequestProtocol.Connect(cs, ct, ca, st) =>
      log.debug(s"Creating a ZooKeeper client for: ${cs}. Attempt ${state.currentAttempt} or $ca...")
      val zk = new ZkConnection(cs, st.length.toInt)
      zk.connect(this)
      context.system.scheduler.scheduleOnce(ct) {
        self ! ZkInternalProtocol.ZkInitialConnectionDeadline()
      }
      val requestor = state.requestor match {
        case Some(ref) => ref
        case None      => sender
      }
      log.debug("Awaiting connection...")
      become(awaitingConnection, state.copy(
        connectRequest = Some(req),
        connection = Some(zk),
        requestor = Some(requestor)))

    case req @ ZkRequestProtocol.IsSaslEnabled() =>
      zkSasl(req) match {
        case Left(saslResponse) => sender ! saslResponse
        case Right(error) => sender ! error
      }

    case anyOther =>
      log.warning(s"Unexpected message: $anyOther.")
      sender ! ZkResponseProtocol.Disconnected
  }

  def awaitingConnection(state: ZkClientState): Receive = {
    case ZkInternalProtocol.ZkInitialConnectionDeadline() =>
      withMaybeConnectRequest(state) { connectRequest =>
        if (state.currentAttempt < connectRequest.connectionAttempts) {
          state.connection.map { conn =>
            Try {
              conn.close()
            } recover {
              case e: Exception => log.error(s"Error while closing the client: $e")
            }
          }
          val nextState = state.copy(currentAttempt = state.currentAttempt+1)
          become(connect, nextState)
          context.system.scheduler.scheduleOnce(100 milliseconds) {
            log.debug(s"Reconnecting...")
            self ! connectRequest
          }
        } else {
          log.error("All connection requests failed. Stopping.")
          throw new ZkClientConnectionFailedException()
        }
      }

    case ZkRequestProtocol.Stop() =>
      log.debug("Stop requested...")
      become(disconnecting, state)
      context.system.stop(self)

    case ZkInternalProtocol.ZkConnectionSuccessful() =>
      withMaybeConnectRequest(state) { connectRequest =>
        withMaybeRequestor(state) { requestor =>
          log.debug("ZooKeeper client connected.")
          requestor ! ZkResponseProtocol.Connected(connectRequest)
          become(connected, state)
        }
      }

    case ZkRequestProtocol.Connect(_, _, _, _) =>
      sender ! ZkResponseProtocol.AwaitingConnection

    case req @ ZkRequestProtocol.IsSaslEnabled() =>
      zkSasl(req) match {
        case Left(saslResponse) => sender ! saslResponse
        case Right(error) => sender ! error
      }

    case anyOther =>
      sender ! ZkResponseProtocol.AwaitingConnection
  }

  def connected(state: ZkClientState): Receive = {
    case ZkRequestProtocol.Connect(_, _, _, _) =>
      withMaybeConnectRequest(state) { connectRequest =>
        sender ! ZkResponseProtocol.Connected(connectRequest)
      }

    case ZkInternalProtocol.ZkInitialConnectionDeadline => // ignore, we are connected

    case ZkInternalProtocol.ZkProcessStateChange(event) =>
      withMaybeRequestor(state) { requestor =>
        requestor ! ZkResponseProtocol.StateChange(event)
      }

    case ZkInternalProtocol.ZkProcessDataChange(event) =>
      Option(event.getPath) match {
        case Some(path) =>
          state.dataChangeSubscriptions.getOrDefault(path, List.empty[ActorRef].asJava).asScala.foreach { ref =>
            ref ! ZkResponseProtocol.DataChange(event)
          }
        case None =>
          log.warning(s"Expected the path to be not null when processing data change event: $event.")
      }

    case ZkInternalProtocol.ZkProcessChildChange(event) =>
      Option(event.getPath) match {
        case Some(path) =>
          state.childChangeSubscriptions.getOrDefault(path, List.empty[ActorRef].asJava).asScala.foreach { ref =>
            ref ! ZkResponseProtocol.ChildChange(event)
          }
        case None =>
          log.warning(s"Expected the path to be not null when processing child change event: $event.")
      }

    case ZkRequestProtocol.Stop() =>
      log.debug("Stop requested...")
      become(disconnecting, state)
      context.system.stop(self)

    case ZkRequestProtocol.SetSerializer(serializer) =>
      become(connected, state.copy(serializer = serializer))

    // TODO: recursive delete?

    case req @ ZkRequestProtocol.AddAuthInfo(scheme, authInfo) =>
      withMaybeConnection(state) { connection =>
        Try { connection.addAuthInfo(scheme, authInfo) } match {
          case Success(_) => sender ! ZkResponseProtocol.AuthInfoAdded(req)
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.CreatePersistent(path, maybeData, acls, sequential) =>
      withMaybeConnection(state) { connection =>
        val mode = if (sequential) CreateMode.PERSISTENT_SEQUENTIAL else CreateMode.PERSISTENT
        zkCreate(connection, state.serializer, path, maybeData, acls, mode) match {
          case Success(v) => sender ! ZkResponseProtocol.Created(req, v)
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.CreateEphemeral(path, maybeData, acls, sequential) =>
      withMaybeConnection(state) { connection =>
        val mode = if (sequential) CreateMode.EPHEMERAL_SEQUENTIAL else CreateMode.EPHEMERAL
        zkCreate(connection, state.serializer, path, maybeData, acls, mode) match {
          case Success(v) => sender ! ZkResponseProtocol.Created(req, v)
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.GetChildren(path, maybeWatch) =>
      withMaybeConnection(state) { connection =>
        val watch = maybeWatch match {
          case Some(v) => v
          case None    => hasListeners(state, path)
        }
        zkGetChildren(connection, path, watch) match {
          case Success(v) => sender ! ZkResponseProtocol.Children(req, v.asScala.toList)
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.CountChildren(path) =>
      withMaybeConnection(state) { connection =>
        zkGetChildren(connection, path, hasListeners(state, path)) match {
          case Success(v) => sender ! ZkResponseProtocol.ChildrenCount(req, v.size())
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.IsExisting(path) =>
      withMaybeConnection(state) { connection =>
        zkExists(connection, path, hasListeners(state, path)) match {
          case Success(v) =>
            val status = if (v) PathExistenceStatus.Exists else PathExistenceStatus.DoesNotExist
            sender ! ZkResponseProtocol.Existence(req, status)
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.ReadData(path, maybeStat, maybeWatch, noneIfNoPath) =>
      withMaybeConnection(state) { connection =>
        val stat = maybeStat match {
          case Some(v) => v
          case None => null
        }
        val watch = maybeWatch match {
          case Some(v) => v
          case None    => hasListeners(state, path)
        }
        zkReadData(connection, path, stat, watch) match {
          case Success(v) => Option(v) match {
            case Some(bytes) => sender ! ZkResponseProtocol.Data(req, Some(state.serializer.deserialize(bytes)))
            case None        => sender ! ZkResponseProtocol.Data(req, None)
          }
          case Failure(e) =>
            if (noneIfNoPath) {
              e match {
                case _: KeeperException.NoNodeException => sender ! ZkResponseProtocol.Data(req, None)
                case _                                  => sender ! ZkResponseProtocol.OperationError(req, e)
              }
            } else {
              sender ! ZkResponseProtocol.OperationError(req, e)
            }
        }
      }

    case req @ ZkRequestProtocol.WriteData(path, maybeData, expectedVersion) =>
      withMaybeConnection(state) { connection =>
        val data = maybeData match {
          case Some(data) => state.serializer.serialize(data)
          case None => null
        }
        Try { connection.writeDataReturnStat(path, data, expectedVersion) } match {
          case Success(v) => sender ! ZkResponseProtocol.Written(req, v)
          case Failure(e)   => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.CreatedWhen(path) =>
      // don't use underlaying functionality, do it like the IZkConnection does it
      withMaybeConnection(state) { connection =>
        val stat = new Stat()
        zkReadData(connection, path, stat, false) match {
          case Success(_) => sender ! ZkResponseProtocol.CreatedAt(req, stat.getCtime)
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.Delete(path, version) =>
      withMaybeConnection(state) { connection =>
        Try { connection.delete(path, version) } match {
          case Success(_) =>  sender ! ZkResponseProtocol.Deleted(req)
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.GetAcl(path) =>
      withMaybeConnection(state) { connection =>
        Try { connection.getAcl(path) } match {
          case Success(v) => sender ! ZkResponseProtocol.AclData(req, AclEntry(v.getKey.asScala.toList, v.getValue))
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.SetAcl(path, acl) =>
      withMaybeConnection(state) { connection =>
        val stat = new Stat()
        zkReadData(connection, path, stat, false) match {
          case Success(_) =>
            Try { connection.setAcl(path, acl.asJava, stat.getAversion) } match {
              case Success(_) => sender ! ZkResponseProtocol.AclSet(req)
              case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
            }
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.Multi(ops) =>
      withMaybeConnection(state) { connection =>
        Try { connection.multi(ops.asJava) } match {
          case Success(v) => sender ! ZkResponseProtocol.MultiResponse(req, v.asScala.toList)
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.IsSaslEnabled() =>
      zkSasl(req) match {
        case Left(saslResponse) => sender ! saslResponse
        case Right(error) => sender ! error
      }

    case req @ ZkRequestProtocol.SubscribeChildChanges(path) =>
      withMaybeConnection(state) { connection =>
        val members = state.childChangeSubscriptions.getOrDefault(path, new util.ArrayList[ActorRef]())
        if (!members.contains(sender)) {
          members.add(sender)
        }
        state.childChangeSubscriptions.put(path, members)
        zkExists(connection, path, true)
        become(connected, state)
        metricChildChangeSubscribersCount.inc()
        sender ! ZkResponseProtocol.SubscriptionSuccess(req)
      }

    case req @ ZkRequestProtocol.SubscribeDataChanges(path) =>
      withMaybeConnection(state) { connection =>
        val members = state.dataChangeSubscriptions.getOrDefault(path, new util.ArrayList[ActorRef]())
        if (!members.contains(sender)) {
          members.add(sender)
        }
        state.dataChangeSubscriptions.put(path, members)
        zkExists(connection, path, true)
        become(connected, state)
        metricDataChangeSubscribersCount.inc()
        sender ! ZkResponseProtocol.SubscriptionSuccess(req)
      }

    case req @ ZkRequestProtocol.UnsubscribeChildChanges(path) =>
      val members = state.childChangeSubscriptions.getOrDefault(path, new util.ArrayList[ActorRef]())
      if (members.remove(sender)) {
        state.childChangeSubscriptions.put(path, members)
      }
      become(connected, state)
      metricChildChangeSubscribersCount.dec()
      sender ! ZkResponseProtocol.UnsubscriptionSuccess(req)

    case req @ ZkRequestProtocol.UnsubscribeDataChanges(path) =>
      val members = state.dataChangeSubscriptions.getOrDefault(path, List(sender).asJava)
      if (members.remove(sender)) {
        state.dataChangeSubscriptions.put(path, members)
      }
      become(connected, state)
      metricDataChangeSubscribersCount.dec()
      sender ! ZkResponseProtocol.UnsubscriptionSuccess(req)

    case req @ ZkRequestProtocol.Metrics() =>
      sender ! ZkResponseProtocol.Metrics(req, metricsAsMap())

    case anyOther =>
      log.warning(s"Unsupported message: $anyOther.")
  }

  def disconnecting(state: ZkClientState): Receive = {
    case _ => // ignore, we're disconnecting anyway...
  }

  private var stateData: Option[ZkClientState] = None
  private def become(f: (ZkClientState) => Receive, state: ZkClientState): Unit = {
    stateData = Some(state)
    context.become(f.apply(state))
  }

  override def postStop(): Unit = {
    super.postStop()
    stateData.map { data =>
      data.connection.map { conn =>
        Try {
          log.info("Attempting closing ZooKeeper client connection...")
          conn.close()
          log.info("ZooKeeper client connection closed.")
        }.recover {
          case e => log.warning(s"There was an exception while stopping the ZooKeeper connection: ${e.getMessage}.")
        }
      }
    }
  }

  // -- INTERNAL API:

  private def hasListeners(state: ZkClientState, path: String): Boolean = {
    val empty = List.empty[ActorRef].asJava
    if (state.childChangeSubscriptions.getOrDefault(path, empty).size() > 0
      || state.dataChangeSubscriptions.getOrDefault(path, empty).size() > 0) {
      return true
    }
    false
  }

  private def zkSasl(request: ZkRequestProtocol.IsSaslEnabled): Either[ZkResponseProtocol.Sasl, ZkResponseProtocol.OperationError] = {
    if (!System.getProperty(SaslProperties.ZkSaslCLlient, "true").toBoolean) {
      log.warning(s"SASL disabled explicitly with ${SaslProperties.ZkSaslCLlient}.")
      Left(ZkResponseProtocol.Sasl(request, SaslStatus.DisabledExplicitly))
    } else {
      val loginConfigFile = System.getProperty(SaslProperties.JavaLoginConfigParam)
      Option(loginConfigFile) match {
        case Some(v) if v.length > 0 => {
          if (!new File(v).canRead) {
            Right(ZkResponseProtocol.OperationError(request, ZkSaslConfigurationFileException(s"File $v cannot be read.")))
          } else {
            Try {
              val zkLoginContextName = System.getProperty(SaslProperties.ZkLoginContextNameKey, "Client")
              Option(Configuration.getConfiguration().getAppConfigurationEntry(zkLoginContextName)) match {
                case Some(_) => Left(ZkResponseProtocol.Sasl(request, SaslStatus.Enabled))
                case None => Left(ZkResponseProtocol.Sasl(request, SaslStatus.Disabled))
              }
            }.recover {
              case e => Right(ZkResponseProtocol.OperationError(request, e))
            }.get
          }
        }
        case _ => Left(ZkResponseProtocol.Sasl(request, SaslStatus.Disabled))
      }
    }
  }

  private def zkCreate(connection: ZkConnection, serializer: ZkSerializer, path: String, maybeData: Option[Any], acls: List[ACL], mode: CreateMode): Try[String] = {
    val data = maybeData match {
      case Some(value) => serializer.serialize(value)
      case None => null
    }
    Try { connection.create(path, data, acls.asJava, mode) }
  }

  private def zkGetChildren(connection: ZkConnection, path: String, watch: Boolean): Try[java.util.List[String]] = {
    Try { connection.getChildren(path, watch) }
  }

  private def zkExists(connection: ZkConnection, path: String, watch: Boolean): Try[Boolean] = {
    Try { connection.exists(path, watch) }
  }

  private def zkReadData(connection: ZkConnection, path: String, stat: Stat, watch: Boolean): Try[Array[Byte]] = {
    Try { connection.readData(path, stat, watch) }
  }

  @throws[ZkClientInvalidStateException]
  private def withMaybeRequestor(state: ZkClientState)(f: (ActorRef) => Unit): Unit = {
    state.requestor match {
      case Some(requestor) => f.apply(requestor)
      case None            => throw ZkClientInvalidStateException(ZkClientMessages.ConnectRequestorMissing)
    }
  }

  @throws[ZkClientInvalidStateException]
  private def withMaybeConnectRequest(state: ZkClientState)(f: (ZkRequestProtocol.Connect) => Unit): Unit = {
    state.connectRequest match {
      case Some(connectRequest) => f.apply(connectRequest)
      case None                 => throw ZkClientInvalidStateException(ZkClientMessages.ConnectRequestMissing)
    }
  }

  @throws[ZkClientInvalidStateException]
  private def withMaybeConnection(state: ZkClientState)(f: (ZkConnection) => Unit): Unit = {
    state.connection match {
      case Some(connection) => f.apply(connection)
      case None             => throw ZkClientInvalidStateException(ZkClientMessages.ConnectionMissing)
    }
  }

  // -- METRICS

  private[zk] val registry = new MetricRegistry
  private[zk] val metricDataChangeSubscribersCount = registry.counter(ZkClientMetricNames.DataChangeSubscribersCount.name)
  private[zk] val metricChildChangeSubscribersCount = registry.counter(ZkClientMetricNames.ChildChangeSubscribersCount.name)

  private[zk] def metricsAsMap(): Map[String, Long] = {
    registry.getCounters.asScala.map { tupple =>
      (tupple._1, tupple._2.getCount)
    }.toMap ++ registry.getGauges.asScala.map { tupple =>
      (tupple._1, tupple._2.getValue.asInstanceOf[Int].toLong)
    }
  }

}
