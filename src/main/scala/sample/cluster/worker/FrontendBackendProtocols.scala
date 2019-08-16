package sample.cluster.worker

object FrontendBackendProtocols {

  final case class SparkJob( text: String )
  final case class JobFailed( Reason: String, job: SparkJob )
  case object BackendRegistration

}
