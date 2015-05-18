package streaming.perf.util

import java.io.File
import java.lang.management.ManagementFactory
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.io.{Codec, Source}

case class MemoryStat(className: String, instances: Long, bytes: Long)

object MemoryStat {

  def fromLine(line: String): MemoryStat = {
    // E.g.,
    // 5:         10563         253512  java.lang.String
    val Array(_, instances, bytes, className) = line.trim.split("\\s+")
    MemoryStat(className, instances.toLong, bytes.toLong)
  }
}

case class MemoryLeakInfo(className: String, firstStat: MemoryStat, lastStat: MemoryStat) {

  val instancesTimes = lastStat.instances.toDouble / firstStat.instances
  val bytesTimes = lastStat.bytes.toDouble / firstStat.bytes

  def generateReport(): String = {
    s"$className: (#instances," +
      s" ${firstStat.instances} -> ${lastStat.instances}, ${percentage(instancesTimes)}%)," +
      s" (#bytes, ${lastStat.instances} -> ${lastStat.bytes}, ${percentage(bytesTimes)}%)"
  }

  private def percentage(d: Double): String = {
    "+%.2f".format(d * 100 - 100)
  }
}

class MemoryLeakAnalyzer(memoryLeakThreshold: Double = 2.0) {

  def analyze(histoFile: File): Boolean = {
    // The file format is
    //
    // Tue May 12 22:44:38 UTC 2015
    //
    //    num     #instances         #bytes  class name
    //    ----------------------------------------------
    //    1:         10651        1153656  [C
    //    2:           815         519880  [B
    //    3:          1622         391456  [I
    //    4:          3216         355016  java.lang.Class
    // ...
    // Tue May 12 22:54:38 UTC 2015
    //
    //    num     #instances         #bytes  class name
    //    ----------------------------------------------
    //    1:         10651        1153656  [C
    //    2:           815         519880  [B
    //    3:          1622         391456  [I
    //    4:          3216         355016  java.lang.Class
    // ...
    //
    val dateFormat = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy")
    var currentDate: Date = null
    var stats = ArrayBuffer[MemoryStat]()
    val records = ArrayBuffer[(Date, Seq[MemoryStat])]()
    for (line <- Source.fromFile(histoFile)(Codec.UTF8).getLines().map(_.trim)
         if !shouldSkip(line)) {
      try {
        val firstChar = line(0)
        if (firstChar.isDigit) {
          // data line
          stats += MemoryStat.fromLine(line)
        } else {
          // date line
          if (currentDate != null) {
            require(stats.nonEmpty)
            records += currentDate -> stats
            currentDate = null
            stats = ArrayBuffer[MemoryStat]()
          }
          currentDate = dateFormat.parse(line)
        }
      } catch {
        case e: Exception => new RuntimeException(s"Cannot parse $line", e)
      }
    }
    if (currentDate != null) {
      require(stats.nonEmpty)
      records += currentDate -> stats
    }

    if (records.isEmpty) {
      throw new IllegalArgumentException(
        "Cannot find any histogram. Please run your application longer")
    }

    val sortedRecords = records.sortBy(_._1).flatMap(_._2)
    val classNames = records.flatMap(_._2.map(_.className)).toSet
    val memoryLeakInfos =
      classNames.toSeq.flatMap(analyzeClass(sortedRecords, _)).sortBy(_.instancesTimes).reverse
    if (memoryLeakInfos.isEmpty) {
      println("No memory leak detected")
      false
    } else {
      println("Detect memory leak")
      memoryLeakInfos.foreach(x => println(x.generateReport()))
      true
    }
  }

  private def analyzeClass(
      sortedRecords: Seq[MemoryStat], className: String): Option[MemoryLeakInfo] = {
    val firstStat = sortedRecords.find(_.className == className).get
    val lastStat = sortedRecords(sortedRecords.lastIndexWhere(_.className == className))
    if (lastStat.instances >= firstStat.instances * memoryLeakThreshold) {
      Some(MemoryLeakInfo(className, firstStat, lastStat))
    } else {
      None
    }
  }

  private def shouldSkip(line: String): Boolean = {
    line.isEmpty ||
      line.startsWith("num")
      line.startsWith("----")
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("need the histo file")
      sys.exit(1)
    }
    val histoFile = new File(args(0))
    new MemoryLeakAnalyzer().analyze(histoFile)
  }
}

/**
 * @param logDir the log dir to put all "histogram" and "heap dump" files
 * @param histoInterval the time interval to wait before generating the next "histogram"
 * @param heapDumpInterval the time interval to wait before generating the next "heap dump"
 * @param topNum the top num of classes which will be included in the "histogram"
 * @param memoryLeakThreshold threshold of "last_stat / first_stat" that we think it's a memory leak
 */
class MemoryLeakDetector(
    logDir: File,
    histoInterval: FiniteDuration = 10.minutes,
    heapDumpInterval: FiniteDuration = 1.hour,
    topNum: Int = 50,
    memoryLeakThreshold: Double = 2.0) {

  private val pid = {
    // See http://maxrohde.com/2012/12/13/java-get-process-id-three-approaches/
    ManagementFactory.getRuntimeMXBean().getName().split("@")(0).toInt
  }

  println("Params:")
  println("logDir: " + logDir.getCanonicalPath)
  println("histoInterval: " + histoInterval)
  println("heapDumpInterval: " + heapDumpInterval)
  println("topNum: " + topNum)
  println("memoryLeakThreshold: " + memoryLeakThreshold)
  println("pid: " + pid)

  if (!logDir.exists()) {
    if (!logDir.mkdirs()) {
      throw new RuntimeException("Cannot create " + logDir)
    }
  }

  private val analyzer = new MemoryLeakAnalyzer()

  private val scheduler = Executors.newSingleThreadScheduledExecutor()

  private val histoFile = new File(logDir, s"histo.$pid")

  def start(): Unit = {
    if (histoFile.exists()) {
      println(s"Deleting $histoFile")
      histoFile.delete()
    }

    scheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        val now = new Date()
        // Output the current time to "histoFile" at first and then redirect the output of
        // "jmap -histo:live" to "histoFile"
        val cmdarray = Array("/bin/bash",  "-c",
          s"echo '$now' >> '${histoFile.getCanonicalPath}' &&" +
            s" jmap -histo:live $pid | head -n ${topNum + 3} >> '${histoFile.getCanonicalPath}'")
        runCommand(cmdarray)
      }
    }, histoInterval.toNanos, histoInterval.toNanos, TimeUnit.NANOSECONDS)

    scheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        val now = new Date()
        val heapDumpFile = new File(logDir, s"heap.bin.$pid.$now")
        val cmdarray =
          Array("jmap", s"-dump:live,format=b,file=${heapDumpFile.getCanonicalPath}", pid.toString)
        runCommand(cmdarray)
      }
    }, heapDumpInterval.toNanos, heapDumpInterval.toNanos, TimeUnit.NANOSECONDS)
  }

  def stop(): Unit = {
    scheduler.shutdown()
    scheduler.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)
    analyzer.analyze(histoFile)
  }

  /**
   * Run a shell command and redirect stdout and stderr of the new process to the console.
   */
  private def runCommand(cmdarray: Array[String]): Int = {
    println(cmdarray.mkString(" "))
    val process = Runtime.getRuntime.exec(cmdarray)
    // Redirect stdout and stderr
    new Thread {
      override def run(): Unit = {
        val input = process.getInputStream()
        for (line <- Source.fromInputStream(input)(Codec.UTF8).getLines()) {
          println(line)
        }
      }
    }.start()
    new Thread {
      override def run(): Unit = {
        val input = process.getErrorStream()
        for (line <- Source.fromInputStream(input)(Codec.UTF8).getLines()) {
          println(line)
        }
      }
    }.start()
    process.waitFor()
  }

}
