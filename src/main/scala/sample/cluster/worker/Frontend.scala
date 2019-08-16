package sample.cluster.worker

import akka.actor.{ Actor, ActorRef, ActorSystem, Props, Stash, Terminated }
import com.typesafe.config.ConfigFactory
import FrontendBackendProtocols._
import akka.cluster

import scala.language.postfixOps

class Frontend extends Actor with Stash {

  var backend = IndexedSeq.empty[ActorRef]
  var jobCounter = 0

  def receive = {

    case job: SparkJob if backend.isEmpty =>
      sender() ! JobFailed(" Service Unavailable, try again later ", job)
      stash()

    case job: SparkJob =>
      jobCounter += 1
      backend(jobCounter % backend.size) forward job

    case BackendRegistration if !backend.contains(sender()) =>
      context watch sender()
      backend = backend :+ sender()
      println("Backend with ActorRef = ${a}, Registered")
      unstashAll()

    case Terminated(a) =>
      backend.filter(_ != a)
      println(s"Backend with ActorRef = ${a}, Terminated ")
  }
}

object Frontend {

  def main(args: Array[String]): Unit = {
    // Override the configuration of the port when specified as program argument
    // To use artery instead of netty, change to "akka.remote.artery.canonical.port"
    // See https://doc.akka.io/docs/akka/current/remoting-artery.html for details
    val port = if (args.isEmpty) "0" else args(0)
    val config = ConfigFactory.parseString(s"""
        akka.remote.netty.tcp.port=$port
        """)
      .withFallback(ConfigFactory.parseString("akka.cluster.roles = [frontend]"))
      .withFallback(ConfigFactory.load())

    println(config)

    val system = ActorSystem("ClusterSystem", config)
    val frontend = system.actorOf(Props[Frontend], name = "frontend")

    println("Frontend instantiated")
    for (i <- 0 until 1000) {
      frontend ! SparkJob("message number = " + i.toString)
    }
  }
}
