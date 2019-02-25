/**
  * test
  */
package org.apache.spark.streaming.dstream

import java.io.{IOException, ObjectInputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.util.{SerializableConfiguration, TimeStampedHashMap, Utils}

import scala.collection.mutable
import scala.reflect.ClassTag

private[streaming]
class LatestFileInputDStream[K, V, F <: NewInputFormat[K, V]](
                                                               _ssc: StreamingContext,
                                                               directory: String,
                                                               filter: Path => Boolean = LatestFileInputDStream.defaultFilter,
                                                               newFilesOnly: Boolean = true,
                                                               conf: Option[Configuration] = None,
                                                               rememberDuration: Duration = Minutes(60))
                                                             (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F])
  extends InputDStream[(K, V)](_ssc) {

  private val serializableConfOpt = conf.map(new SerializableConfiguration(_))

  /**
    * Minimum duration of remembering the information of selected files. Defaults to 60 seconds.
    *
    * Files with mod times older than this "window" of remembering will be ignored. So if new
    * files are visible within this window, then the file will get selected in the next batch.
    */
  private val minRememberDurationS = {
    Seconds(ssc.conf.getTimeAsSeconds("spark.streaming.fileStream.minRememberDuration",
      ssc.conf.get("spark.streaming.minRememberDuration", "60s")))
  }

  /**
    * 定义一个变量，记录每次读取文件（HDFS）后生成RDD的时间
    */
  private[streaming] var rddGenerateTime: Long = 0
  private[streaming] var rememberRDDs: Option[RDD[(K, V)]] = _

  // This is a def so that it works during checkpoint recovery:
  private def clock = ssc.scheduler.clock

  // Data to be saved as part of the streaming checkpoints
  protected[streaming] override val checkpointData = new LatestFileInputStreamCheckpointData

  // Initial ignore threshold based on which old, existing files in the directory (at the time of
  // starting the streaming application) will be ignored or considered
  private val initialModTimeIgnoreThreshold = if (newFilesOnly) clock.getTimeMillis() else 0L

  /*
   * Make sure that the information of files selected in the last few batches are remembered.
   * This would allow us to filter away not-too-old files which have already been recently
   * selected and processed.
   */
  private val numBatchesToRemember = LatestFileInputDStream
    .calculateNumBatchesToRemember(slideDuration, minRememberDurationS)
  private val durationToRemember = slideDuration * numBatchesToRemember
  remember(durationToRemember)

  // Map of batch-time to selected file info for the remembered batches
  // This is a concurrent map because it's also accessed in unit tests
  @transient private[streaming] var batchTimeToSelectedFiles =
    new mutable.HashMap[Time, Array[String]]

  // Set of files that were selected in the remembered batches
  @transient private var recentlySelectedFiles = new mutable.HashSet[String]()

  // Read-through cache of file mod times, used to speed up mod time lookups
  @transient private var fileToModTime = new TimeStampedHashMap[String, Long](true)

  // Timestamp of the last round of finding files
  @transient private var lastNewFileFindingTime = 0L

  @transient private var _path: Path = null
  @transient private var _fs: FileSystem = null

  override def start() {}

  override def stop() {}

  /**
    * Finds the files that were modified since the last time this method was called and makes
    * a union RDD out of them. Note that this maintains the list of files that were processed
    * in the latest modification time in the previous call to this method. This is because the
    * modification time returned by the FileStatus API seems to return times only at the
    * granularity of seconds. And new files may have the same modification time as the
    * latest modification time in the previous call to this method yet was not reported in
    * the previous call.
    */
  override def compute(validTime: Time): Option[RDD[(K, V)]] = {
    if (rddGenerateTime == 0 || Duration(validTime.milliseconds - rddGenerateTime) >= durationToRemember) {
      //从HDFS读取数据生成的RDD的周期为durationToRemember
      //      val newFiles = findNewFiles(validTime.milliseconds) //这个方法会加载目录里面的所有文件
      //如果不是第一次加载，为了能识别出slideDuration这段时间内新增的文件，所以要给rddGenerateTime加上slide
      //这个时间要在findNewFiles(rddGenerateTime)中用到，是判断的截止时间（即认为rddGenerateTime为当前时间）
      if (rddGenerateTime != 0) {
        rddGenerateTime += durationToRemember.milliseconds
      }
      val newFiles = findNewFiles(rddGenerateTime) //这个方法会加载目录里面的所有文件
      logInfo("New files at time " + validTime + ":\n" + newFiles.mkString("\n"))
      batchTimeToSelectedFiles.synchronized {
        batchTimeToSelectedFiles += ((validTime, newFiles))
      }
      recentlySelectedFiles ++= newFiles
      val rdds = Some(filesToRDD(newFiles))
      rddGenerateTime = validTime.milliseconds //记录从HDFS读取数据的时间
      val metadata = Map(
          "files" -> newFiles.toList,
          StreamInputInfo.METADATA_KEY_DESCRIPTION -> newFiles.mkString("\n"))
      val inputInfo = StreamInputInfo(id, 0, metadata)
      ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
      rememberRDDs = rdds //记录从HDFS读取数据生成的RDD
      rdds
    } else {
      /**
        * 这样在DStream.getOrCompute方法里面会把这个RDD重复的放入generateRDDs这个HashMap里面.不过没有关系，因为generateRDDs里面的RDD
        * 在超过rememberDuration之后会将RDD从generateRDDs里面删除，并且将RDD所占用的内存块也删除。在实际执行的时候不会发生重复删除内存块，因为删除内存块之前会先检查内存块是否存在
        * 第一次删除之后，
        * 以后再删除的时候发现内存块已经不存在了，会直接返回，不会重复删除
        */
      //useRememberRDDTime = validTime.milliseconds
      rememberRDDs
    }
  }

  /** Clear the old time-to-files mappings along with old RDDs */
  protected[streaming] override def clearMetadata(time: Time) {
    batchTimeToSelectedFiles.synchronized {
      val oldFiles = batchTimeToSelectedFiles.filter(_._1 < (time - rememberDuration))
      batchTimeToSelectedFiles --= oldFiles.keys
      recentlySelectedFiles --= oldFiles.values.flatten
      logInfo("Cleared " + oldFiles.size + " old files that were older than " +
        (time - rememberDuration) + ": " + oldFiles.keys.mkString(", "))
      logDebug("Cleared files are:\n" +
        oldFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n"))
    }
    // Delete file mod times that weren't accessed in the last round of getting new files
    fileToModTime.clearOldValues(lastNewFileFindingTime - 1)
  }

  /**
    * Find new files for the batch of `currentTime`. This is done by first calculating the
    * ignore threshold for file mod times, and then getting a list of files filtered based on
    * the current batch time and the ignore threshold. The ignore threshold is the max of
    * initial ignore threshold and the trailing end of the remember window (that is, which ever
    * is later in time).
    */
  private def findNewFiles(currentTime: Long): Array[String] = {
    try {
      lastNewFileFindingTime = clock.getTimeMillis()

      // Calculate ignore threshold
      val modTimeIgnoreThreshold = math.max(
        initialModTimeIgnoreThreshold, // initial threshold based on newFilesOnly setting
        currentTime - durationToRemember.milliseconds // trailing end of the remember window
      )
      logDebug(s"Getting new files for time $currentTime, " +
        s"ignoring files older than $modTimeIgnoreThreshold")

      val newFileFilter = new PathFilter {
        def accept(path: Path): Boolean = isNewFile(path, currentTime, modTimeIgnoreThreshold)
      }
      val directoryFilter = new PathFilter {
        override def accept(path: Path): Boolean = fs.getFileStatus(path).isDirectory
      }
      val directories = fs.globStatus(directoryPath, directoryFilter).map(_.getPath)
      val newFiles = directories.flatMap(dir =>
        fs.listStatus(dir, newFileFilter).map(_.getPath.toString))
      val timeTaken = clock.getTimeMillis() - lastNewFileFindingTime
      logInfo("Finding new files took " + timeTaken + " ms")
      logDebug("# cached file times = " + fileToModTime.size)
      if (timeTaken > slideDuration.milliseconds) {
        logWarning(
          "Time taken to find new files exceeds the batch size. " +
            "Consider increasing the batch size or reducing the number of " +
            "files in the monitored directory."
        )
      }
      newFiles
    } catch {
      case e: Exception =>
        logWarning("Error finding new files", e)
        reset()
        Array.empty
    }
  }

  /**
    * Identify whether the given `path` is a new file for the batch of `currentTime`. For it to be
    * accepted, it has to pass the following criteria.
    * - It must pass the user-provided file filter.
    * - It must be newer than the ignore threshold. It is assumed that files older than the ignore
    * threshold have already been considered or are existing files before start
    * (when newFileOnly = true).
    * - It must not be present in the recently selected files that this class remembers.
    * - It must not be newer than the time of the batch (i.e. `currentTime` for which this
    * file is being tested. This can occur if the driver was recovered, and the missing batches
    * (during downtime) are being generated. In that case, a batch of time T may be generated
    * at time T+x. Say x = 5. If that batch T contains file of mod time T+5, then bad things can
    * happen. Let's say the selected files are remembered for 60 seconds.  At time t+61,
    * the batch of time t is forgotten, and the ignore threshold is still T+1.
    * The files with mod time T+5 are not remembered and cannot be ignored (since, t+5 > t+1).
    * Hence they can get selected as new files again. To prevent this, files whose mod time is more
    * than current batch time are not considered.
    */
  private def isNewFile(path: Path, currentTime: Long, modTimeIgnoreThreshold: Long): Boolean = {
    val pathStr = path.toString
    // Reject file if it does not satisfy filter
    if (!filter(path)) {
      logDebug(s"$pathStr rejected by filter")
      return false
    }
    //第一次读取目录默认加载里面所有的文件
    if (currentTime == 0) {
      logDebug(s"$pathStr is first load")
      return true
    } else {
      // Reject file if it was created before the ignore time
      val modTime = getFileModTime(path)
      if (modTime <= modTimeIgnoreThreshold) {
        // Use <= instead of < to avoid SPARK-4518
        logDebug(s"$pathStr ignored as mod time $modTime <= ignore time $modTimeIgnoreThreshold")
        return false
      }
      // Reject file if mod time > current batch time
      if (modTime > currentTime) {
        logDebug(s"$pathStr not selected as mod time $modTime > current time $currentTime")
        return false
      }
      // Reject file if it was considered earlier
      if (recentlySelectedFiles.contains(pathStr)) {
        logDebug(s"$pathStr already considered")
        return false
      }
      logDebug(s"$pathStr accepted with mod time $modTime")
    }
    return true
  }

  /**
    * 这个方法也是存储NewHadoopRDD到内存的关键代码
    *
    * @param files
    * @return
    */
  private def filesToRDD(files: Seq[String]): RDD[(K, V)] = {
    val fileRDDs = files.map { file =>
      val rdd = serializableConfOpt.map(_.value) match {
        case Some(config) => {
          context.sparkContext.newAPIHadoopFile(
            file,
            fm.runtimeClass.asInstanceOf[Class[F]],
            km.runtimeClass.asInstanceOf[Class[K]],
            vm.runtimeClass.asInstanceOf[Class[V]],
            config)
            .map { case (k, v) =>
              val newKey = new LongWritable(k.asInstanceOf[LongWritable].get).asInstanceOf[K]
              val newText = new Text(v.asInstanceOf[Text].toString).asInstanceOf[V]
              (newKey, newText) //因为NewHadoopRDD每个元素都占用了同一块内存，所以必须复制每个元素的k、v才能将这个RDD cache到内存

            }
        }
        case None => {
          context.sparkContext.newAPIHadoopFile[K, V, F](file)
            .map { case (k, v) =>
              val newKey = new LongWritable(k.asInstanceOf[LongWritable].get).asInstanceOf[K]
              val newText = new Text(v.asInstanceOf[Text].toString).asInstanceOf[V]
              (newKey, newText) //因为NewHadoopRDD每个元素都占用了同一块内存，所以必须复制每个元素的k、v才能将这个RDD cache到内存</span>


            }
        }
      }
      if (rdd.partitions.size == 0) {
        logError("File " + file + " has no data in it. Spark Streaming can only ingest " +
          "files that have been \"moved\" to the directory assigned to the file stream. " +
          "Refer to the streaming programming guide for more details.")
      }
      rdd
    }
    new UnionRDD(context.sparkContext, fileRDDs)
  }

  /** Get file mod time from cache or fetch it from the file system */
  private def getFileModTime(path: Path) = {
    fileToModTime.getOrElseUpdate(path.toString, fs.getFileStatus(path).getModificationTime())
  }

  private def directoryPath: Path = {
    if (_path == null) _path = new Path(directory)
    _path
  }

  private def fs: FileSystem = {
    if (_fs == null) _fs = directoryPath.getFileSystem(ssc.sparkContext.hadoopConfiguration)
    _fs
  }

  private def reset() {
    _fs = null
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    logDebug(this.getClass().getSimpleName + ".readObject used")
    ois.defaultReadObject()
    generatedRDDs = new mutable.HashMap[Time, RDD[(K, V)]]()
    batchTimeToSelectedFiles = new mutable.HashMap[Time, Array[String]]
    recentlySelectedFiles = new mutable.HashSet[String]()
    fileToModTime = new TimeStampedHashMap[String, Long](true)
  }

  /**
    * A custom version of the DStreamCheckpointData that stores names of
    * Hadoop files as checkpoint data.
    */
  private[streaming]
  class LatestFileInputStreamCheckpointData extends DStreamCheckpointData(this) {

    private def hadoopFiles = data.asInstanceOf[mutable.HashMap[Time, Array[String]]]

    override def update(time: Time) {
      hadoopFiles.clear()
      batchTimeToSelectedFiles.synchronized {
        hadoopFiles ++= batchTimeToSelectedFiles
      }
    }

    override def cleanup(time: Time) {}

    override def restore() {
      hadoopFiles.toSeq.sortBy(_._1)(Time.ordering).foreach {
        case (t, f) =>
          // Restore the metadata in both files and generatedRDDs
          logInfo("Restoring files for time " + t + " - " +
            f.mkString("[", ", ", "]"))
          batchTimeToSelectedFiles.synchronized {
            batchTimeToSelectedFiles += ((t, f))
          }
          recentlySelectedFiles ++= f
          generatedRDDs += ((t, filesToRDD(f)))
      }
    }

    override def toString: String = {
      "[\n" + hadoopFiles.size + " file sets\n" +
        hadoopFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n") + "\n]"
    }
  }

}

private[streaming]
object LatestFileInputDStream {

  def defaultFilter(path: Path): Boolean = !path.getName().startsWith(".")

  /**
    * Calculate the number of last batches to remember, such that all the files selected in
    * at least last minRememberDurationS duration can be remembered.
    */
  def calculateNumBatchesToRemember(batchDuration: Duration,
                                    minRememberDurationS: Duration): Int = {
    math.ceil(minRememberDurationS.milliseconds.toDouble / batchDuration.milliseconds).toInt
  }
}