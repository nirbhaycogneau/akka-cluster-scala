package sample.cluster.worker

import akka.actor.{Actor, ActorRef, ActorSystem, Props, RootActorPath, Stash, Terminated}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import akka.cluster.{Member, MemberStatus}
import com.typesafe.config.ConfigFactory
import FrontendBackendProtocols._
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}
import sample.cluster.worker.BackendWorkerProtocols.{WorkIsDone, WorkerRequestsWork}

class Backend extends Actor {

  val cluster = Cluster(context.system)

  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])
  override def postStop(): Unit = cluster.unsubscribe(self)

  private var routees = Vector.fill(5) {
    val r = context.system.actorOf(Worker.props(self))
    context.watch(r)
    r
  }

  var jobCount = 0

  def route(task : SparkJob ) = routees( jobCount % routees.length ) ! task

  def removeRoutee(a: ActorRef) = {
    routees = routees.filter( _ != a )
  }

  def addRoutee(a: ActorRef) = {
    routees = routees :+ a
  }

  def receive: Receive = {

    case SparkJob(text) =>
      route(SparkJob(text))

    case Terminated(a) =>
      removeRoutee(a)
      val r = context.system.actorOf(Worker.props(self))
      context.watch(r)
      addRoutee(r)

    case WorkerRequestsWork(workerId) =>
      router.route(text, sender())

    case WorkIsDone(workerId, workId, result) =>
      router = router.addRoutee(sender())

    case state: CurrentClusterState =>
      state.members.filter(_.status == MemberStatus.Up ) foreach register

    case MemberUp(m) => register(m)
  }

  def register(member: Member) = {
    println("Applied for registration")
    if (member.hasRole("frontend"))
      context.actorSelection(RootActorPath(member.address)/ "user"/ "frontend") ! BackendRegistration
  }
}

object Backend {
  def main(args: Array[String]): Unit = {
    // Override the configuration of the port when specified as program argument
    // To use artery instead of netty, change to "akka.remote.artery.canonical.port"
    // See https://doc.akka.io/docs/akka/current/remoting-artery.html for details
    val port = if (args.isEmpty) "0" else args(0)
    val config = ConfigFactory.parseString(s"""
        akka.remote.netty.tcp.port=$port
        """)
      .withFallback(ConfigFactory.parseString("akka.cluster.roles = [backend]"))
      .withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    system.actorOf(Props[Backend], name = "backend")

    println("Backend Instantiated")
  }
}
