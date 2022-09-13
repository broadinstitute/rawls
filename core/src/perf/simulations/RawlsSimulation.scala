package default

import scala.sys.process._

trait RawlsSimulation extends Simulation {

  //function to help us generate TSVs per-run
  def fileGenerator(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  //Helpers to set up the run

  val lines = scala.io.Source.fromFile("../user-files/config.txt").getLines

  // uses currently gcloud-auth'd user
  val accessToken = ("gcloud auth print-access-token" !!) replaceAll("\n", "")
  val numUsers = lines.next.toInt

}

case class SubmissionStatus(
  submissionId: String,
  submissionStatus: String,
  workflowStatuses: Vector[String]
)
