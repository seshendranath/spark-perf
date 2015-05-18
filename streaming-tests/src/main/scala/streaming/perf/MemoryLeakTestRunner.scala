package streaming.perf

import java.io.File

import scala.concurrent.duration._

import streaming.perf.util.MemoryLeakDetector

object MemoryLeakTestRunner {

  def main(args: Array[String]) {
    val logDir = new File(sys.props.getOrElse("spark-perf.streaming.log", "."))
    val histoInterval =
      sys.props.getOrElse("spark-perf.streaming.memory-leak.histo-interval", "600").toInt.seconds
    val heapDumpInterval =
      sys.props.getOrElse("spark-perf.streaming.memory-leak.heap-dump-interval", "3600").toInt.seconds
    val topNum = sys.props.getOrElse("spark-perf.streaming.memory-leak.top-num", "50").toInt
    val memoryLeakThreshold = sys.props.getOrElse("spark-perf.streaming.memory-leak.threshold", "2.0").toDouble
    val memoryLeakDetector = new MemoryLeakDetector(logDir, histoInterval, heapDumpInterval, topNum, memoryLeakThreshold)
    memoryLeakDetector.start()
    TestRunner.main(args)
    memoryLeakDetector.stop()
    System.out.flush()
  }

}
