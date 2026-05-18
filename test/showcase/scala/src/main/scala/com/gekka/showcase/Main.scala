package com.gekka.showcase

object Main {
  def main(args: Array[String]): Unit = {
    val node = sys.props.getOrElse("showcase.node",
      sys.error("required: -Dshowcase.node=s1|s2|s3|s4|s5"))
    val portStr = sys.props.getOrElse("showcase.port",
      sys.error("required: -Dshowcase.port=<int>"))
    val seedHostPort = sys.props.getOrElse("showcase.seeds",
      sys.error("required: -Dshowcase.seeds=host1:port1,host2:port2"))

    println(s"--- SHOWCASE MAIN BOOTSTRAP: node=$node port=$portStr seeds=$seedHostPort ---")
    // Actual ActorSystem startup added in Task 3
  }
}
