package uk.co.appministry.akka.zk

import java.io.File
import java.util
import java.util.{List => JList}
import javax.security.auth.login.Configuration

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.stream.actor.ActorPublisher
import com.codahale.metrics.MetricRegistry
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
  final val ZkSaslClient = "zookeeper.sasl.client"
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
  val ConnectionAttempts = 2
  val SessionTimeout = 30 seconds
}

object ZkClientMetricNames {
  sealed abstract class MetricName(val name: String)
  case object ChildChangePathsObservedCount extends MetricName("child-change-paths-observed-count")
  case object DataChangePathsObservedCount extends MetricName("data-change-paths-observed-count")
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
    * Add the specified scheme:auth information to this connection.
    * @param scheme
    * @param authInfo
    */
  final case class AddAuthInfo(val scheme: String, val authInfo: Array[Byte]) extends Request

  /**
    * Connect to the server.
    * @param connectionString comma separated host:port pairs, each corresponding to a zk server. e.g.
    *                         "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If the optional chroot suffix is used
    *                         the example would look like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a" where
    *                         the client would be rooted at "/app/a" and all paths would be relative to this
    *                         root - ie getting/setting/etc... "/foo/bar" would result in operations being run on
    *                         "/app/a/foo/bar" (from the server perspective).
    * @param connectionAttempts how many times to retry a failed connect attempt
    * @param sessionTimeout session timeout
    */
  final case class Connect(val connectionString: String = ZkClientProtocolDefaults.ConnectionString,
                           val connectionAttempts: Int = ZkClientProtocolDefaults.ConnectionAttempts,
                           val sessionTimeout: FiniteDuration = ZkClientProtocolDefaults.SessionTimeout) extends Request

  /**
    * Return the number the children of the node of the given path.
    * @param path the given path for the node
    */
  final case class CountChildren(val path: String) extends Request

  /**
    * Create an ephemeral node with the given path.
    * @param path the given path for the node
    * @param data the initial data for the node
    * @param acl the acl for the node
    * @param sequential should the node be sequential
    */
  final case class CreateEphemeral(val path: String,
                                   val data: Option[Any] = None,
                                   val acl: List[ACL] = ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala.toList,
                                   val sequential: Boolean = false) extends CreateRequest

  /**
    * Create a persistent node with the given path.
    * @param path the given path for the node
    * @param data the initial data for the node
    * @param acl the acl for the node
    * @param sequential should the node be sequential
    */
  final case class CreatePersistent(val path: String,
                                    val data: Option[Any] = None,
                                    val acl: List[ACL] = ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala.toList,
                                    val sequential: Boolean = false) extends CreateRequest

  /**
    * Get the timestamp of when the node has been created.
    * @param path the given path for the node
    */
  final case class CreatedWhen(val path: String) extends Request

  /**
    * Delete a node with a given path.
    * @param path the given path for the node
    * @param version the expected node version
    */
  final case class Delete(val path: String, val version: Int = -1) extends Request

  /**
    * Return the ACL and stat of the node of the given path.
    * @param path the given path for the node
    */
  final case class GetAcl(val path: String) extends Request

  /**
    * Return the list of the children of the node of the given path.
    * @param path the given path for the node
    * @param watch watch for changes
    */
  final case class GetChildren(val path: String, val watch: Option[Boolean] = None) extends Request

  /**
    * Check if the node exists.
    * @param path the given path for the node
    */
  final case class IsExisting(val path: String) extends Request

  /**
    * Check if SASL is enabled.
    */
  final case class IsSaslEnabled() extends Request

  /**
    * Return the data and the stat of the node of the given path.
    * @param path the given path for the node
    * @param noneIfNoPath return None if node at path does not exist instead of returning an OperationError
    */
  final case class ReadData(val path: String,
                            val noneIfNoPath: Boolean = false) extends Request

  /**
    * Request metrics snapshot.
    */
  final case class Metrics() extends Request

  /**
    * Executes multiple ZooKeeper operations or none of them.
    * @param ops An iterable that contains the operations to be done. These should be created using the factory methods
    *            on Op.
    */
  final case class Multi(val ops: List[Op]) extends Request

  /**
    * Set the ACL for the node of the given path if such a node exists and the given version matches the version of the
    * node.
    * @param path the given path for the node
    * @param acl the new acl for the node
    */
  final case class SetAcl(val path: String, val acl: List[ACL]) extends Request

  /**
    * Set the read / write serializer for this client.
    * @param serializer the serializer
    */
  final case class SetSerializer(val serializer: ZkSerializer) extends Request

  /**
    * Stop the ZooKeeper client. Disconnect from ZooKeeper, if necessary. Will stop the actor.
    */
  final case class Stop()

  /**
    * Subscribe to the children changes of the znode at the path.
    * @param path the given path for the node
    */
  final case class SubscribeChildChanges(val path: String) extends SubscribeRequest

  /**
    * Subscribe to the data changes of the znode at the path.
    * @param path the given path for the node
    */
  final case class SubscribeDataChanges(val path: String) extends SubscribeRequest

  /**
    * Unsubscribe from the children changes of the znode at the path.
    * @param path the given path for the node
    */
  final case class UnsubscribeChildChanges(val path: String) extends UnsubscribeRequest

  /**
    * Unsubscribe from the data changes of the znode at the path.
    * @param path the given path for the node
    */
  final case class UnsubscribeDataChanges(val path: String) extends UnsubscribeRequest

  /**
    * Set the data for the node of the given path if such a node exists and the given version matches the version of the
    * node (if the given version is -1, it matches any node's versions).
    * @param path the given path for the node
    * @param data new data for the node
    * @param expectedVersion expected version
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
    * [[ZkRequestProtocol.GetAcl]] response.
    * @param request the original request
    * @param entry acl data entry
    */
  final case class AclData(val request: ZkRequestProtocol.GetAcl, val entry: AclEntry) extends Response

  /**
    * [[ZkRequestProtocol.SetAcl]] response.
    * @param request the original request
    */
  final case class AclSet(val request: ZkRequestProtocol.SetAcl) extends Response

  /**
    * [[ZkRequestProtocol.AddAuthInfo]] response.
    * @param request the original request
    */
  final case class AuthInfoAdded(val request: ZkRequestProtocol.AddAuthInfo) extends Response

  /**
    * ZooKeeper client is not yet connected. Expected after issuing [[ZkRequestProtocol.Connect]], until the client
    * successfully connects.
    */
  final case class AwaitingConnection() extends Response

  /**
    * [[ZkRequestProtocol.GetChildren]] response.
    * @param request the original request
    * @param result list of znode's children
    */
  final case class Children(val request: ZkRequestProtocol.GetChildren, val result: List[String]) extends Response

  /**
    * [[ZkRequestProtocol.CountChildren]] response.
    * @param request the original request
    * @param result number of znode's children, if znode exists
    */
  final case class ChildrenCount(val request: ZkRequestProtocol.CountChildren, val result: Int) extends Response

  /**
    * Issued to the ZkClient parent when the client successfully connects.
    * @param request the original request
    */
  final case class Connected(val request: ZkRequestProtocol.Connect) extends Response

  /**
    * [[ZkRequestProtocol.CreatePersistent]] and [[ZkRequestProtocol.CreateEphemeral]] response.
    * @param request the original request
    * @param result the actual path of the created node
    */
  final case class Created(val request: ZkRequestProtocol.CreateRequest, val result: String) extends Response

  /**
    * [[ZkRequestProtocol.CreatedWhen]] response.
    * @param request the original request
    * @param timestamp timestamp of when the znode has been created, if znode exists
    */
  final case class CreatedAt(val request: ZkRequestProtocol.CreatedWhen, val timestamp: Long) extends Response

  /**
    * [[ZkRequestProtocol.ReadData]] response.
    * @param request the original request
    * @param data the data of the znode
    * @param stat the stat of the znode
    */
  final case class Data(val request: ZkRequestProtocol.ReadData, val data: Option[Any], val stat: Option[Stat])
    extends Response

  /**
    * Issued to the parent of the ZkClient when the [[ZkRequestProtocol.Connect]] has not been issued yet.
    */
  final case class Disconnected() extends Response

  /**
    * [[ZkRequestProtocol.Delete]] response.
    * @param request the original request
    */
  final case class Deleted(val request: ZkRequestProtocol.Delete) extends Response

  /**
    * [[ZkRequestProtocol.IsExisting]] response.
    * @param request the original request
    * @param status znode's existence status
    */
  final case class Existence(val request: ZkRequestProtocol.IsExisting, val status: PathExistenceStatus.Status)
    extends Response

  /**
    * [[ZkRequestProtocol.Metrics]] response.
    * @param request the original request
    * @param metrics current metrics snapshot
    */
  final case class Metrics(val request: ZkRequestProtocol.Metrics, val metrics: Map[String, Long]) extends Response

  /**
    * [[ZkRequestProtocol.Multi]] response.
    * @param request the original request
    * @param results
    */
  final case class MultiResponse(val request: ZkRequestProtocol.Multi, val results: List[OpResult]) extends Response

  /**
    * Any failed ZooKeeper request is presented with this object.
    * @param request request for which the exception occured
    * @param reason  original exception
    */
  final case class OperationError(val request: ZkRequestProtocol.Request, val reason: Throwable) extends Response

  /**
    * [[ZkRequestProtocol.IsSaslEnabled]] response.
    * @param request the original request
    * @param status SASL status
    */
  final case class Sasl(val request: ZkRequestProtocol.IsSaslEnabled, val status: SaslStatus.Status) extends Response

  /**
    * [[ZkRequestProtocol.SubscribeRequest]] response.
    * @param request the original request
    */
  final case class SubscriptionSuccess(val request: ZkRequestProtocol.SubscribeRequest) extends Response

  /**
    * [[ZkRequestProtocol.UnsubscribeRequest]] response.
    * @param request the original request
    */
  final case class UnsubscriptionSuccess(val request: ZkRequestProtocol.UnsubscribeRequest) extends Response

  /**
    * [[ZkRequestProtocol.WriteData]] response.
    * @param request the original request
    * @param stat the stat of the node
    */
  final case class Written(val request: ZkRequestProtocol.WriteData, val stat: Stat) extends Response

}

object ZkClientStreamProtocol {

  sealed trait StreamResponse

  /**
    * Subscriber event for child change subscriptions.
    * @param event original ZooKeeper event
    */
  final case class ChildChange(val event: WatchedEvent) extends StreamResponse

  /**
    * Subscriber event for data change subscriptions.
    * @param event original ZooKeeper event
    */
  final case class DataChange(val event: WatchedEvent) extends StreamResponse

  /**
    * Issued to the parent of the ZkClient when the client connection state changes.
    * @param event the original request
    */
  final case class StateChange(val event: WatchedEvent) extends StreamResponse

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
                         val connection: Option[ZooKeeper] = None,
                         val serializer: ZkSerializer = new SimpleSerializer,
                         val dataSubscriptions: JList[String] = new util.ArrayList[String](),
                         val childSubscriptions: JList[String] = new util.ArrayList[String]())

/**
  * Akka ZooKeeper client.
  *
  * TODO: usage...
  * TODO: Intead of using ActorPublisher, consider using the [[org.reactivestreams.Publisher]].
  *       According to the Akka docs, ActorPublisher may be removed in the future.
  */
class ZkClientActor extends Actor with ActorPublisher[ZkClientStreamProtocol.StreamResponse] with ActorLogging with ZkClientWatcher {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  def receive = connect(ZkClientState(1, None, None))

  def connect(state: ZkClientState): Receive = {
    case req @ ZkRequestProtocol.Connect(connectionString, connectionAttempts, sessionTimeout) =>
      log.debug(s"Creating a ZooKeeper client for: ${connectionString}. Attempt ${state.currentAttempt} or $connectionAttempts...")
      val zk = new ZooKeeper(connectionString, sessionTimeout.length.toInt, this)
      context.system.scheduler.scheduleOnce(sessionTimeout) {
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
      log.warning(s"Unexpected message in awaitingConnection: $anyOther.")
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

    case ZkRequestProtocol.Connect(_, _, _) =>
      sender ! ZkResponseProtocol.AwaitingConnection

    case req @ ZkRequestProtocol.IsSaslEnabled() =>
      zkSasl(req) match {
        case Left(saslResponse) => sender ! saslResponse
        case Right(error) => sender ! error
      }

    case anyOther =>
      log.debug(s"Received an unexpected message: $anyOther.")
      sender ! ZkResponseProtocol.AwaitingConnection
  }

  def connected(state: ZkClientState): Receive = {
    case ZkRequestProtocol.Connect(_, _, _) =>
      withMaybeConnectRequest(state) { connectRequest =>
        sender ! ZkResponseProtocol.Connected(connectRequest)
      }

    case ZkInternalProtocol.ZkInitialConnectionDeadline => // ignore, we are connected

    case ZkInternalProtocol.ZkProcessStateChange(event) =>
      streamMaybeProduce(ZkClientStreamProtocol.StateChange(event))

    case ZkInternalProtocol.ZkProcessDataChange(event) =>
      Option(event.getPath) match {
        case Some(path) =>
          if (state.dataSubscriptions.contains(path)) {
            streamMaybeProduce(ZkClientStreamProtocol.DataChange(event))
          }
        case None =>
          log.warning(s"Expected the path to be not null when processing data change event: $event.")
      }

    case ZkInternalProtocol.ZkProcessChildChange(event) =>
      Option(event.getPath) match {
        case Some(path) =>
          if ( state.childSubscriptions.contains(path) ) {
            streamMaybeProduce(ZkClientStreamProtocol.ChildChange(event))
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
          case None    => isPathWatchable(state, path)
        }
        zkGetChildren(connection, state, path) match {
          case Success(v) => sender ! ZkResponseProtocol.Children(req, v.asScala.toList)
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.CountChildren(path) =>
      withMaybeConnection(state) { connection =>
        zkGetChildren(connection, state, path) match {
          case Success(v) => sender ! ZkResponseProtocol.ChildrenCount(req, v.size())
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.IsExisting(path) =>
      withMaybeConnection(state) { connection =>
        zkExists(connection, state, path) match {
          case Success(v) =>
            val status = if (v) PathExistenceStatus.Exists else PathExistenceStatus.DoesNotExist
            sender ! ZkResponseProtocol.Existence(req, status)
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.ReadData(path, noneIfNoPath) =>
      withMaybeConnection(state) { connection =>
        zkReadData(connection, state, path) match {
          case Success(v) =>
            Try {
              val maybeData = Option(state.serializer.deserialize(v._1)) match {
                case Some(data) => Some(data)
                case None => None
              }
              sender ! ZkResponseProtocol.Data(req, maybeData, Option(v._2))
            }.recover {
              case e: Throwable => sender ! ZkResponseProtocol.OperationError(req, e)
            }
          case Failure(e) =>
            if (noneIfNoPath) {
              e match {
                case _: KeeperException.NoNodeException => sender ! ZkResponseProtocol.Data(req, None, None)
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
        zkWriteData(connection, state.serializer, path, data, expectedVersion) match {
          case Success(v) =>
            sender ! ZkResponseProtocol.Written(req, v)
            if (isPathWatchable(state, path)) { // reinstall the watch, if necessary
              zkExists(connection, state, path)
            }
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.CreatedWhen(path) =>
      withMaybeConnection(state) { connection =>
        zkReadData(connection, state, path) match {
          case Success(v) => sender ! ZkResponseProtocol.CreatedAt(req, v._2.getCtime)
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
        val stat = new Stat()
        Try { connection.getACL(path, stat) } match {
          case Success(v) => sender ! ZkResponseProtocol.AclData(req, AclEntry(v.asScala.toList, stat))
          case Failure(e) => sender ! ZkResponseProtocol.OperationError(req, e)
        }
      }

    case req @ ZkRequestProtocol.SetAcl(path, acl) =>
      withMaybeConnection(state) { connection =>
        zkReadData(connection, state, path) match {
          case Success(v) =>
            Try { connection.setACL(path, acl.asJava, v._2.getAversion) } match {
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
        if (!state.childSubscriptions.contains(path)) {
          state.childSubscriptions.add(path)
          zkExists(connection, state, path)
          metricChildChangPathsObservedCount.inc()
          become(connected, state)
        }
        sender ! ZkResponseProtocol.SubscriptionSuccess(req)
      }

    case req @ ZkRequestProtocol.SubscribeDataChanges(path) =>
      withMaybeConnection(state) { connection =>
        if (!state.dataSubscriptions.contains(path)) {
          state.dataSubscriptions.add(path)
          zkExists(connection, state, path)
          metricDataChangePathsObservedCount.inc()
          become(connected, state)
        }
        sender ! ZkResponseProtocol.SubscriptionSuccess(req)
      }

    case req @ ZkRequestProtocol.UnsubscribeChildChanges(path) =>
      if (state.childSubscriptions.remove(path)) {
        metricChildChangPathsObservedCount.dec()
        become(connected, state)
      }
      sender ! ZkResponseProtocol.UnsubscriptionSuccess(req)

    case req @ ZkRequestProtocol.UnsubscribeDataChanges(path) =>
      if (state.dataSubscriptions.remove(path)) {
        metricDataChangePathsObservedCount.dec()
        become(connected, state)
      }
      sender ! ZkResponseProtocol.UnsubscriptionSuccess(req)

    case req @ ZkRequestProtocol.Metrics() =>
      sender ! ZkResponseProtocol.Metrics(req, metricsAsMap())

    case anyOther =>
      log.debug(s"Unexpected message in connected: $anyOther.")
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

  private def streamMaybeProduce(data: ZkClientStreamProtocol.StreamResponse): Unit = {
    if (isActive && totalDemand > 0) {
      onNext(data)
    }
  }

  private def isPathWatchable(state: ZkClientState, path: String): Boolean = {
    (state.childSubscriptions.contains(path) || state.dataSubscriptions.contains(path))
  }

  private def zkSasl(request: ZkRequestProtocol.IsSaslEnabled): Either[ZkResponseProtocol.Sasl, ZkResponseProtocol.OperationError] = {
    if (!System.getProperty(SaslProperties.ZkSaslClient, "true").toBoolean) {
      log.warning(s"SASL disabled explicitly with ${SaslProperties.ZkSaslClient}.")
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

  private def zkCreate(connection: ZooKeeper, serializer: ZkSerializer, path: String, maybeData: Option[Any], acls: List[ACL], mode: CreateMode): Try[String] = {
    val data = maybeData match {
      case Some(value) => serializer.serialize(value)
      case None => null
    }
    Try { connection.create(path, data, acls.asJava, mode) }
  }

  private def zkGetChildren(connection: ZooKeeper, state: ZkClientState, path: String): Try[java.util.List[String]] = {
    Try { connection.getChildren(path, isPathWatchable(state, path)) }
  }

  private def zkExists(connection: ZooKeeper, state: ZkClientState, path: String): Try[Boolean] = {
    Try { Option(connection.exists(path, isPathWatchable(state, path))) != None }
  }

  private def zkReadData(connection: ZooKeeper, state: ZkClientState, path: String): Try[Tuple2[Array[Byte], Stat]] = {
    val stat = new Stat()
    Try { (connection.getData(path, isPathWatchable(state, path), stat), stat) }
  }

  private def zkWriteData(connection: ZooKeeper, serializer: ZkSerializer, path: String, data: Any, expectedVersion: Int): Try[Stat] = {
    Try { connection.setData(path, serializer.serialize(data), expectedVersion) }
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
  private def withMaybeConnection(state: ZkClientState)(f: (ZooKeeper) => Unit): Unit = {
    state.connection match {
      case Some(connection) => f.apply(connection)
      case None             => throw ZkClientInvalidStateException(ZkClientMessages.ConnectionMissing)
    }
  }

  // -- METRICS

  private[zk] val registry = new MetricRegistry
  private[zk] val metricDataChangePathsObservedCount = registry.counter(ZkClientMetricNames.DataChangePathsObservedCount.name)
  private[zk] val metricChildChangPathsObservedCount = registry.counter(ZkClientMetricNames.ChildChangePathsObservedCount.name)

  private[zk] def metricsAsMap(): Map[String, Long] = {
    registry.getCounters.asScala.map { tupple =>
      (tupple._1, tupple._2.getCount)
    }.toMap ++ registry.getGauges.asScala.map { tupple =>
      (tupple._1, tupple._2.getValue.asInstanceOf[Int].toLong)
    }
  }

}
