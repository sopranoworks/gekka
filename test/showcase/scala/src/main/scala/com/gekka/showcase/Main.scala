package com.gekka.showcase

object Main {
  def main(args: Array[String]): Unit = {
    val node = sys.props.getOrElse("showcase.node",
      sys.error("required: -Dshowcase.node=s1|s2|s3|s4|s5"))
    ShowcaseNode.start(node)
    Thread.currentThread().join() // block until JVM is killed
  }
}
