package sample.cluster.worker
/**
  * @author ${Nirbhay}
  */
object WorkerApp {

  def main(args : Array[String]) {
    Frontend.main(Seq("2551").toArray)
    Backend.main(Seq("2552").toArray)
    Backend.main(Array.empty)
    Backend.main(Array.empty)
    Frontend.main(Array.empty)
  }
}

