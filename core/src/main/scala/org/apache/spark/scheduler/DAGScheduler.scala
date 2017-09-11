/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.io.NotSerializableException
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Stack}
import scala.concurrent.duration._
import scala.language.existentials
import scala.language.postfixOps
import scala.util.control.NonFatal

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.storage._
import org.apache.spark.util._
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat

/**
  DAGScheduler是一个面向stage的高层次的调度层（相对于TaskScheduler），
它计算每一个job的各个stage的DAG（一个job有多个stage
stage的划分依据是宽依赖），并且跟踪哪一个RDD，哪一个stage的输出被计算出来了，寻找一个最小化的调度策略。
它会以一个stageSet的形式提交stages到底层的TaskScheduling中，从而使任务在集群上运行

  除了计算stages的DAG，它也会根据当前的缓存的状态确定最好的位置来运行每一个task，
  并且它会处理由于shuffle过程中输出文件丢失的错误，在这种情况下旧的stages将会被重新提交
  但是不是由于sghuffle过程产生的错误将会由TaskScheduler处理，taskScheduler将会尝试重复提交几次在确定取消整个stage之前



  job : A job is triggered by an action, like count() or saveAsTextFile(). Click on a job to see information about the stages of tasks inside it.
理解了吗，所谓一个 job，就是由一个 rdd 的 action 触发的动作，
可以简单的理解为，当你需要执行一个 rdd 的 action 的时候，会生成一个 job。
stage : stage 是一个 job 的组成单位，
就是说，一个 job 会被切分成 1 个或 1 个以上的 stage，然后各个 stage 会按照执行顺序依次执行。
  stage按照宽依赖划分
task : A unit of work within a stage, corresponding to one RDD partition。
即 stage 下的一个任务执行单元，一般来说，一个 rdd 有多少个 partition，就会有多少个 task，因为每一个 task 只是处理一个 partition 上的数据。
  */

/**
 * The high-level scheduling layer that implements stage-oriented scheduling. It computes a DAG of
 * stages for each job, keeps track of which RDDs and stage outputs are materialized, and finds a
 * minimal schedule to run the job. It then submits stages as TaskSets to an underlying
 * TaskScheduler implementation that runs them on the cluster.
 *
 * In addition to coming up with a DAG of stages, this class also determines the preferred
 * locations to run each task on, based on the current cache status, and passes these to the
 * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
 * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
 * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
 * a small number of times before cancelling the whole stage.
 *
 * Here's a checklist to use when making or reviewing changes to this class:
 *
 *  - When adding a new data structure, update `DAGSchedulerSuite.assertDataStructuresEmpty` to
 *    include the new structure. This will help to catch memory leaks.
 */
private[spark]
class DAGScheduler(
    private[scheduler] val sc: SparkContext,
    private[scheduler] val taskScheduler: TaskScheduler,
    listenerBus: LiveListenerBus,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv,
    clock: Clock = new SystemClock())
  extends Logging {

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env)
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)

  private[scheduler] val metricsSource: DAGSchedulerSource = new DAGSchedulerSource(this)

  private[scheduler] val nextJobId = new AtomicInteger(0)
  private[scheduler] def numTotalJobs: Int = nextJobId.get()
  private val nextStageId = new AtomicInteger(0)

  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]
  private[scheduler] val shuffleToMapStage = new HashMap[Int, ShuffleMapStage]
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]

  // Stages we need to run whose parents aren't done
  private[scheduler] val waitingStages = new HashSet[Stage]

  // Stages we are running right now
  private[scheduler] val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  private[scheduler] val failedStages = new HashSet[Stage]

  private[scheduler] val activeJobs = new HashSet[ActiveJob]

  /**
   * Contains the locations that each RDD's partitions are cached on.  This map's keys are RDD ids
   * and its values are arrays indexed by partition numbers. Each array value is the set of
   * locations where that RDD partition is cached.
   *
   * All accesses to this map should be guarded by synchronizing on it (see SPARK-4454).
   */
  private val cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]

  // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
  // every task. When we detect a node failing, we note the current epoch number and failed
  // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
  //
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
  private val failedEpoch = new HashMap[String, Long]

  private [scheduler] val outputCommitCoordinator = env.outputCommitCoordinator

  // A closure serializer that we reuse.
  // This is only safe because DAGScheduler runs in a single thread.
  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  /** If enabled, FetchFailed will not cause stage retry, in order to surface the problem. */
  private val disallowStageRetryForTest = sc.getConf.getBoolean("spark.test.noStageRetry", false)

  private val messageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("dag-scheduler-message")

  private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
  taskScheduler.setDAGScheduler(this)

  // Flag to control if reduce tasks are assigned preferred locations
  private val shuffleLocalityEnabled =
    sc.getConf.getBoolean("spark.shuffle.reduceLocality.enabled", false)
  // Number of map, reduce tasks above which we do not assign preferred locations
  // based on map output sizes. We limit the size of jobs for which assign preferred locations
  // as computing the top locations by size becomes expensive.
  private[this] val SHUFFLE_PREF_MAP_THRESHOLD = 1000
  // NOTE: This should be less than 2000 as we use HighlyCompressedMapStatus beyond that
  private[this] val SHUFFLE_PREF_REDUCE_THRESHOLD = 1000

  // Fraction of total map output that must be at a location for it to considered as a preferred
  // location for a reduce task.
  // Making this larger will focus on fewer locations where most data can be read locally, but
  // may lead to more delay in scheduling if those locations are busy.
  private[scheduler] val REDUCER_PREF_LOCS_FRACTION = 0.2

  /**
   * Called by the TaskSetManager to report task's starting.
   */
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }

  /**
   * Called by the TaskSetManager to report that a task has completed
   * and results are being fetched remotely.
   */
  def taskGettingResult(taskInfo: TaskInfo) {
    eventProcessLoop.post(GettingResultEvent(taskInfo))
  }

  /**
   * Called by the TaskSetManager to report task completions or failures.
   */
  def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Map[Long, Any],
      taskInfo: TaskInfo,
      taskMetrics: TaskMetrics): Unit = {
    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, taskInfo, taskMetrics))
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  def executorHeartbeatReceived(
      execId: String,
      taskMetrics: Array[(Long, Int, Int, TaskMetrics)], // (taskId, stageId, stateAttempt, metrics)
      blockManagerId: BlockManagerId): Boolean = {
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, taskMetrics))
    blockManagerMaster.driverEndpoint.askWithRetry[Boolean](
      BlockManagerHeartbeat(blockManagerId), new RpcTimeout(600 seconds, "BlockManagerHeartbeat"))
  }

  /**
   * Called by TaskScheduler implementation when an executor fails.
   */
  def executorLost(execId: String): Unit = {
    eventProcessLoop.post(ExecutorLost(execId))
  }

  /**
   * Called by TaskScheduler implementation when a host is added.
   */
  def executorAdded(execId: String, host: String): Unit = {
    eventProcessLoop.post(ExecutorAdded(execId, host))
  }

  /**
   * Called by the TaskSetManager to cancel an entire TaskSet due to either repeated failures or
   * cancellation of the job itself.
   */
  def taskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]): Unit = {
    eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception))
  }

  private[scheduler]
  def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = cacheLocs.synchronized {
    // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
    if (!cacheLocs.contains(rdd.id)) {
      // Note: if the storage level is NONE, we don't need to get locations from block manager.
      val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
        IndexedSeq.fill(rdd.partitions.length)(Nil)
      } else {
        val blockIds =
          rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
        blockManagerMaster.getLocations(blockIds).map { bms =>
          bms.map(bm => TaskLocation(bm.host, bm.executorId))
        }
      }
      cacheLocs(rdd.id) = locs
    }
    cacheLocs(rdd.id)
  }

  private def clearCacheLocs(): Unit = cacheLocs.synchronized {
    cacheLocs.clear()
  }

  /**
   * Get or create a shuffle map stage for the given shuffle dependency's map side.
   */
  private def getShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    shuffleToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) => stage
      case None =>
        // We are going to register ancestor shuffle dependencies
        registerShuffleDependencies(shuffleDep, firstJobId)
        // Then register current shuffleDep
        val stage = newOrUsedShuffleStage(shuffleDep, firstJobId)
        shuffleToMapStage(shuffleDep.shuffleId) = stage

        stage
    }
  }

  /**
   * Helper function to eliminate some code re-use when creating new stages.
   */
  private def getParentStagesAndId(rdd: RDD[_], firstJobId: Int): (List[Stage], Int) = {
    val parentStages = getParentStages(rdd, firstJobId)
    val id = nextStageId.getAndIncrement()
    (parentStages, id)
  }

  /**
   * Create a ShuffleMapStage as part of the (re)-creation of a shuffle map stage in
   * newOrUsedShuffleStage.  The stage will be associated with the provided firstJobId.
   * Production of shuffle map stages should always use newOrUsedShuffleStage, not
   * newShuffleMapStage directly.
   */
  private def newShuffleMapStage(
      rdd: RDD[_],
      numTasks: Int,
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int,
      callSite: CallSite): ShuffleMapStage = {
    val (parentStages: List[Stage], id: Int) = getParentStagesAndId(rdd, firstJobId)
    val stage: ShuffleMapStage = new ShuffleMapStage(id, rdd, numTasks, parentStages,
      firstJobId, callSite, shuffleDep)

    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(firstJobId, stage)
    stage
  }

  /**
   * Create a ResultStage associated with the provided jobId.
   */
  private def newResultStage(
      rdd: RDD[_],
      numTasks: Int,
      jobId: Int,
      callSite: CallSite): ResultStage = {
    val (parentStages: List[Stage], id: Int) = getParentStagesAndId(rdd, jobId)
    val stage: ResultStage = new ResultStage(id, rdd, numTasks, parentStages, jobId, callSite)

    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

  /**
   * Create a shuffle map Stage for the given RDD.  The stage will also be associated with the
   * provided firstJobId.  If a stage for the shuffleId existed previously so that the shuffleId is
   * present in the MapOutputTracker, then the number and location of available outputs are
   * recovered from the MapOutputTracker
   */
  private def newOrUsedShuffleStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    val numTasks = rdd.partitions.length
    val stage = newShuffleMapStage(rdd, numTasks, shuffleDep, firstJobId, rdd.creationSite)
    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      for (i <- 0 until locs.length) {
        stage.outputLocs(i) = Option(locs(i)).toList // locs(i) will be null if missing
      }
      stage.numAvailableOutputs = locs.count(_ != null)
    } else {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
    }
    stage
  }

  /**
   * Get or create the list of parent stages for a given RDD.  The new Stages will be created with
   * the provided firstJobId.
   */
  private def getParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
    val parents = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of partitions is unknown
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              parents += getShuffleMapStage(shufDep, firstJobId)
            case _ =>
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    parents.toList
  }

  /** Find ancestor missing shuffle dependencies and register into shuffleToMapStage */
  private def registerShuffleDependencies(shuffleDep: ShuffleDependency[_, _, _], firstJobId: Int) {
    val parentsWithNoMapStage = getAncestorShuffleDependencies(shuffleDep.rdd)
    while (parentsWithNoMapStage.nonEmpty) {
      val currentShufDep = parentsWithNoMapStage.pop()
      val stage = newOrUsedShuffleStage(currentShufDep, firstJobId)
      shuffleToMapStage(currentShufDep.shuffleId) = stage
    }
  }

  /** Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet */
  private def getAncestorShuffleDependencies(rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {
    val parents = new Stack[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              if (!shuffleToMapStage.contains(shufDep.shuffleId)) {
                parents.push(shufDep)
              }

              waitingForVisit.push(shufDep.rdd)
            case _ =>
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }

    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    parents
  }

  //获取某个stage的父stage
  /*
  * 反向遍历RDD的DAG图，遇到窄依赖就将依赖的RDD加入到stage中，遇到宽依赖就切开并递归宽依赖的stage
  * */
  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]

    def visit(rdd: RDD[_]) {
      //如果RDD没有调用过该visit方法
      if (!visited(rdd)) {
        visited += rdd
        val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
        if (rddHasUncachedPartitions) {
          //遍历RDD的依赖
          //其实对于每一种shuffle操作，比如groupByKey，reduceByKey，countByKey
          //底层都对应了3个RDD，MapPartitionsRDD，ShuffleRDD，MapPartitionsRDD
          for (dep <- rdd.dependencies) {
            dep match {

                //宽依赖：使用造成宽依赖的这个RDD，创建一个新的stage，并且会将isShuffleMap设置为true
                //默认最后一个stage，不是shuffleMap stage，但是finalStage(ResultStage)之前的所有的stage都是shuffleMapStage
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getShuffleMapStage(shufDep, stage.firstJobId)
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }

                //窄依赖：那么将依赖的RDD压入栈中,等到下一次循环的时候（调用visit(waitingForVisit.pop())方法的时候），
                //再次判断该窄依赖的RDD是否有宽依赖
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }
      }
    }

    //往栈中push传递过来的stage的最后一个RDD
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      //对栈顶的rdd进行调用visit方法
      visit(waitingForVisit.pop())
    }
    missing.toList
  }

  /**
   * Registers the given jobId among the jobs that need the given stage and
   * all of that stage's ancestors.
   */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    def updateJobIdStageIdMapsList(stages: List[Stage]) {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parents: List[Stage] = getParentStages(s.rdd, jobId)
        val parentsWithoutThisJobId = parents.filter { ! _.jobIds.contains(jobId) }
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }
    updateJobIdStageIdMapsList(List(stage))
  }

  /**
   * Removes state for job and any stages that are not needed by any other job.  Does not
   * handle cancelling tasks or notifying the SparkListener about finished jobs/stages/tasks.
   *
   * @param job The job whose state to cleanup.
   */
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob): Unit = {
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    } else {
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
              .format(job.jobId, stageId))
          } else {
            def removeStage(stageId: Int) {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                for ((k, v) <- shuffleToMapStage.find(_._2 == stage)) {
                  shuffleToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId
              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= job.jobId
            if (jobSet.isEmpty) { // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage.resultOfJob = None
  }

  /**
   * Submit a job to the job scheduler and get a JobWaiter object back. The JobWaiter object
   * can be used to block until the the job finishes executing or can be used to cancel the job.
   */
  def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    waiter
  }

  def runJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): Unit = {
    val start = System.nanoTime
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    waiter.awaitResult() match {
      case JobSucceeded =>
        logInfo("Job %d finished: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case JobFailed(exception: Exception) =>
        logInfo("Job %d failed: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }

  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: CallSite,
      timeout: Long,
      properties: Properties): PartialResult[R] = {
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val partitions = (0 until rdd.partitions.length).toArray
    val jobId = nextJobId.getAndIncrement()
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions, callSite, listener, SerializationUtils.clone(properties)))
    listener.awaitResult()    // Will throw an exception if the job fails
  }

  /**
   * Cancel a job that is running or waiting in the queue.
   */
  def cancelJob(jobId: Int): Unit = {
    logInfo("Asked to cancel job " + jobId)
    eventProcessLoop.post(JobCancelled(jobId))
  }

  def cancelJobGroup(groupId: String): Unit = {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessLoop.post(JobGroupCancelled(groupId))
  }

  /**
   * Cancel all jobs that are running or waiting in the queue.
   */
  def cancelAllJobs(): Unit = {
    eventProcessLoop.post(AllJobsCancelled)
  }

  private[scheduler] def doCancelAllJobs() {
    // Cancel all running jobs.
    runningStages.map(_.firstJobId).foreach(handleJobCancellation(_,
      reason = "as part of cancellation of all jobs"))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
    submitWaitingStages()
  }

  /**
   * Cancel all jobs associated with a running or scheduled stage.
   */
  def cancelStage(stageId: Int) {
    eventProcessLoop.post(StageCancelled(stageId))
  }

  /**
   * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
   * the last fetch failure.
   */
  private[scheduler] def resubmitFailedStages() {
    if (failedStages.size > 0) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.firstJobId)) {
        submitStage(stage)
      }
    }
    submitWaitingStages()
  }

  /**
   * Check for waiting or failed stages which are now eligible for resubmission.
   * Ordinarily run on every iteration of the event loop.
   */
  private def submitWaitingStages() {
    // TODO: We might want to run this less often, when we are sure that something has become
    // runnable that wasn't before.
    logTrace("Checking for newly runnable parent stages")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    val waitingStagesCopy = waitingStages.toArray
    waitingStages.clear()
    for (stage <- waitingStagesCopy.sortBy(_.firstJobId)) {
      submitStage(stage)
    }
  }

  /** Finds the earliest-created active job that needs the stage */
  // TODO: Probably should actually find among the active jobs that need this
  // stage the one with the highest priority (highest-priority pool, earliest created).
  // That should take care of at least part of the priority inversion problem with
  // cross-job dependencies.
  private def activeJobForStage(stage: Stage): Option[Int] = {
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted
    jobsThatUseStage.find(jobIdToActiveJob.contains)
  }

  private[scheduler] def handleJobGroupCancelled(groupId: String) {
    // Cancel all jobs belonging to this job group.
    // First finds all active jobs with this group id, and then kill stages for them.
    val activeInGroup = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists {
        _.getProperty(SparkContext.SPARK_JOB_GROUP_ID) == groupId
      }
    }
    val jobIds = activeInGroup.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_, "part of cancelled job group %s".format(groupId)))
    submitWaitingStages()
  }

  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo) {
    // Note that there is a chance that this task is launched after the stage is cancelled.
    // In that case, we wouldn't have the stage anymore in stageIdToStage.
    val stageAttemptId = stageIdToStage.get(task.stageId).map(_.latestInfo.attemptId).getOrElse(-1)
    listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))
    submitWaitingStages()
  }

  private[scheduler] def handleTaskSetFailed(
      taskSet: TaskSet,
      reason: String,
      exception: Option[Throwable]): Unit = {
    stageIdToStage.get(taskSet.stageId).foreach { abortStage(_, reason, exception) }
    submitWaitingStages()
  }

  private[scheduler] def cleanUpAfterSchedulerStop() {
    for (job <- activeJobs) {
      val error = new SparkException("Job cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      // The `toArray` here is necessary so that we don't iterate over `runningStages` while
      // mutating it.
      runningStages.toArray.foreach { stage =>
        markStageAsFinished(stage, Some(stageFailedMessage))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo) {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
    submitWaitingStages()
  }

  /*
    完成了job到stage的转换，每一个job都有一个finalStage
  */
  private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    var finalStage: ResultStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.

      //使用触发job的最后一个RDD，创建finalStage
      //创建好对象后，将finalstage对象加入到内存缓存中
      finalStage = newResultStage(finalRDD, partitions.length, jobId, callSite)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
    if (finalStage != null) {
      //使用finalStage创建一个Job，这个Job的最后一个stage当然就是我们的finalStage
      val job = new ActiveJob(jobId, finalStage, func, partitions, callSite, listener, properties)

      clearCacheLocs()
      logInfo("Got job %s (%s) with %d output partitions".format(
        job.jobId, callSite.shortForm, partitions.length))
      logInfo("Final stage: " + finalStage + "(" + finalStage.name + ")")
      logInfo("Parents of final stage: " + finalStage.parents)
      logInfo("Missing parents: " + getMissingParentStages(finalStage))
      val jobSubmissionTime = clock.getTimeMillis()

      //将job加入到内存缓存中
      jobIdToActiveJob(jobId) = job
      activeJobs += job
      finalStage.resultOfJob = Some(job)
      val stageIds = jobIdToStageIds(jobId).toArray
      val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
      listenerBus.post(
        SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))

      //使用submitStage方法提交finalStage
      submitStage(finalStage)
    }
    submitWaitingStages()
  }

  /** Submits stage, but first recursively submits any missing parents. */
  /*
  stage的划分是由submitStage和getMissingParentStages这两个方法完成的
  * */
  private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        //获取当前stage的父stage
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)

        //只有遇到最初的那个stage的时候才会进行提交，其余的stages都在waitingStages里面
        //如果当前的stage的前面再也没有stage了，那么就可以提交了，否则还需要划分missing中的stage
        if (missing.isEmpty) {
          //如果当前的stage没有父stage了，那么就提交当前的stage
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          submitMissingTasks(stage, jobId.get)
        } else {
          //否则对于当前的satge还需要递归调用submitStage，用来划分它的最后一个RDD的窄依赖还是宽依赖
          //这边相当于一个深度遍历图
          for (parent <- missing) {
            submitStage(parent)
          }
          //最后按照倒序的方式（因为这个方法在递归主体的后面），将stage将入到waitingStages中
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
  }

  /** Called when stage's parents are available and we can now do its task. */
  /*提交stage，并为stage创建一批task，task的数量与partition的数量相同*/
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    logDebug("submitMissingTasks(" + stage + ")")
    // Get our pending tasks and remember them in our pendingTasks entry
    stage.pendingTasks.clear()

    // First figure out the indexes of partition ids to compute.
    //计算需要创建的task的数量
    val (allPartitions: Seq[Int], partitionsToCompute: Seq[Int]) = {
      stage match {
        case stage: ShuffleMapStage =>
          val allPartitions = 0 until stage.numPartitions
          val filteredPartitions = allPartitions.filter { id => stage.outputLocs(id).isEmpty }
          (allPartitions, filteredPartitions)
        case stage: ResultStage =>
          val job = stage.resultOfJob.get
          val allPartitions = 0 until job.numPartitions
          val filteredPartitions = allPartitions.filter { id => !job.finished(id) }
          (allPartitions, filteredPartitions)
      }
    }

    // Create internal accumulators if the stage has no accumulators initialized.
    // Reset internal accumulators only if this stage is not partially submitted
    // Otherwise, we may override existing accumulator values from some tasks
    if (stage.internalAccumulators.isEmpty || allPartitions == partitionsToCompute) {
      stage.resetInternalAccumulators()
    }

    val properties = jobIdToActiveJob.get(stage.firstJobId).map(_.properties).orNull

    //将stage加入runningStages队列
    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    outputCommitCoordinator.stageStart(stage.id)
    val taskIdToLocations = try {
      stage match {
        case s: ShuffleMapStage =>
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          val job = s.resultOfJob.get
          partitionsToCompute.map { id =>
            val p = job.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        stage.makeNewStageAttempt(partitionsToCompute.size)
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
        abortStage(stage, s"Task creation failed: $e\n${e.getStackTraceString}", Some(e))
        runningStages -= stage
        return
    }

    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] = stage match {
        case stage: ShuffleMapStage =>
          closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef).array()
        case stage: ResultStage =>
          closureSerializer.serialize((stage.rdd, stage.resultOfJob.get.func): AnyRef).array()
      }

      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${e.getStackTraceString}", Some(e))
        runningStages -= stage
        return
    }

    //为stage创建指定数量的task，每一个taskSet都包含了这个stage中的所有的任务，taskSets中的每一个任务处理逻辑都是完全一样的
    //不一样的只是对应的处理的数据，这些数据是其对应的数据切片（Partition）
    val tasks: Seq[Task[_]] = try {
      stage match {
        //这边对于finalStage之外的stage，它的isShuffleMap都是true，所以会创建shuffleMapTask
        case stage: ShuffleMapStage =>
          partitionsToCompute.map { id =>
            //给每一个partition创建一个task，给每一个task获取计算的最佳位置
            val locs = taskIdToLocations(id)
            val part = stage.rdd.partitions(id)
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, stage.internalAccumulators)
          }

          //这边处理的是finalStage，这边创建的就是resultTask任务
        case stage: ResultStage =>
          val job = stage.resultOfJob.get
          partitionsToCompute.map { id =>
            val p: Int = job.partitions(id)
            val part = stage.rdd.partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, id, stage.internalAccumulators)
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${e.getStackTraceString}", Some(e))
        runningStages -= stage
        return
    }

    if (tasks.size > 0) {
      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
      stage.pendingTasks ++= tasks
      logDebug("New pending tasks: " + stage.pendingTasks)

      //向TaskSchuduler提交任务，以stage为单位，一个stage对应一个TaskSet
      //在这里我们使用的是taskScheduler的实现类TaskSchedulerImpl
      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptId, stage.firstJobId, properties))
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      markStageAsFinished(stage, None)

      val debugString = stage match {
        case stage: ShuffleMapStage =>
          s"Stage ${stage} is actually done; " +
            s"(available: ${stage.isAvailable}," +
            s"available outputs: ${stage.numAvailableOutputs}," +
            s"partitions: ${stage.numPartitions})"
        case stage : ResultStage =>
          s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})"
      }
      logDebug(debugString)
    }
  }

  /** Merge updates from a task to our local accumulator values */
  private def updateAccumulators(event: CompletionEvent): Unit = {
    val task = event.task
    val stage = stageIdToStage(task.stageId)
    if (event.accumUpdates != null) {
      try {
        Accumulators.add(event.accumUpdates)

        event.accumUpdates.foreach { case (id, partialValue) =>
          // In this instance, although the reference in Accumulators.originals is a WeakRef,
          // it's guaranteed to exist since the event.accumUpdates Map exists

          val acc = Accumulators.originals(id).get match {
            case Some(accum) => accum.asInstanceOf[Accumulable[Any, Any]]
            case None => throw new NullPointerException("Non-existent reference to Accumulator")
          }

          // To avoid UI cruft, ignore cases where value wasn't updated
          if (acc.name.isDefined && partialValue != acc.zero) {
            val name = acc.name.get
            val value = s"${acc.value}"
            stage.latestInfo.accumulables(id) =
              new AccumulableInfo(id, name, None, value, acc.isInternal)
            event.taskInfo.accumulables +=
              new AccumulableInfo(id, name, Some(s"$partialValue"), value, acc.isInternal)
          }
        }
      } catch {
        // If we see an exception during accumulator update, just log the
        // error and move on.
        case e: Exception =>
          logError(s"Failed to update accumulators for $task", e)
      }
    }
  }

  /**
   * Responds to a task finishing. This is called inside the event loop so it assumes that it can
   * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
   */
  private[scheduler] def handleTaskCompletion(event: CompletionEvent) {
    val task = event.task
    val stageId = task.stageId
    val taskType = Utils.getFormattedClassName(task)

    outputCommitCoordinator.taskCompleted(
      stageId,
      task.partitionId,
      event.taskInfo.attemptNumber, // this is a task attempt number
      event.reason)

    // The success case is dealt with separately below, since we need to compute accumulator
    // updates before posting.
    if (event.reason != Success) {
      val attemptId = task.stageAttemptId
      listenerBus.post(SparkListenerTaskEnd(stageId, attemptId, taskType, event.reason,
        event.taskInfo, event.taskMetrics))
    }

    if (!stageIdToStage.contains(task.stageId)) {
      // Skip all the actions if the stage has been cancelled.
      return
    }

    val stage = stageIdToStage(task.stageId)
    event.reason match {
      case Success =>
        listenerBus.post(SparkListenerTaskEnd(stageId, stage.latestInfo.attemptId, taskType,
          event.reason, event.taskInfo, event.taskMetrics))
        stage.pendingTasks -= task
        task match {
          case rt: ResultTask[_, _] =>
            // Cast to ResultStage here because it's part of the ResultTask
            // TODO Refactor this out to a function that accepts a ResultStage
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.resultOfJob match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  updateAccumulators(event)
                  job.finished(rt.outputId) = true
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  //如果作业执行完毕，那么标记该作业已经完成
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(resultStage)
                    //清除作业依赖的资源
                    cleanupStateForJobAndIndependentStages(job)
                    listenerBus.post(
                      SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  try {
                    job.listener.taskSucceeded(rt.outputId, event.result)
                  } catch {
                    case e: Exception =>
                      // TODO: Perhaps we want to mark the resultStage as failed?
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
              case None =>
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }

          case smt: ShuffleMapTask =>
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            updateAccumulators(event)
            val status = event.result.asInstanceOf[MapStatus]
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
              logInfo(s"Ignoring possibly bogus $smt completion from executor $execId")
            } else {
              shuffleStage.addOutputLoc(smt.partitionId, status)
            }


            //当shuffleStage是正在运行的stage并且这个stage也没有被挂起的任务的时候，就说明这个stage结束了
            if (runningStages.contains(shuffleStage) && shuffleStage.pendingTasks.isEmpty) {
              markStageAsFinished(shuffleStage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("failed: " + failedStages)

              // We supply true to increment the epoch number here in case this is a
              // recomputation of the map outputs. In that case, some nodes may have cached
              // locations with holes (from when we detected the error) and will need the
              // epoch incremented to refetch them.
              // TODO: Only increment the epoch number if this is not the first time
              //       we registered these map outputs.

              //任务执行完毕后，发现这个task是一个ShuffleMapTask,那么就需要将计算的结果的输出的信息MapStatus
              //注册到MapoutputTracker中
              mapOutputTracker.registerMapOutputs(
                shuffleStage.shuffleDep.shuffleId,
                shuffleStage.outputLocs.map(list => if (list.isEmpty) null else list.head),
                changeEpoch = true)

              clearCacheLocs()
              //当发现shuffleStage的输出结果不存在的时候，就说明任务可能失败了
              //需要重新提交当前的这个stage
              if (shuffleStage.outputLocs.contains(Nil)) {
                // Some tasks had failed; let's resubmit this shuffleStage
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + shuffleStage + " (" + shuffleStage.name +
                  ") because some of its tasks had failed: " +
                  shuffleStage.outputLocs.zipWithIndex.filter(_._1.isEmpty)
                      .map(_._2).mkString(", "))
                submitStage(shuffleStage)
              } else {
                val newlyRunnable = new ArrayBuffer[Stage]
                //提交下一个stage
                for (shuffleStage <- waitingStages) {
                  logInfo("Missing parents for " + shuffleStage + ": " +
                    //再次获取这个stage的父stage
                    getMissingParentStages(shuffleStage))
                }
                //对于没有父stage的stage提交
                for (shuffleStage <- waitingStages if getMissingParentStages(shuffleStage).isEmpty)
                {
                  newlyRunnable += shuffleStage
                }
                //更改stage的状态
                waitingStages --= newlyRunnable
                runningStages ++= newlyRunnable
                for {
                  //如果有多个stage都满足条件的话，按照id排序，一个一个的来
                  shuffleStage <- newlyRunnable.sortBy(_.id)
                  jobId <- activeJobForStage(shuffleStage)
                } {
                  logInfo("Submitting " + shuffleStage + " (" +
                    shuffleStage.rdd + "), which is now runnable")
                  //提交这个下一个stage
                  submitMissingTasks(shuffleStage, jobId)
                }
              }
            }
          }

      case Resubmitted =>
        logInfo("Resubmitted " + task + ", so marking it as still running")
        stage.pendingTasks += task

      case FetchFailed(bmAddress, shuffleId, mapId, reduceId, failureMessage) =>
        val failedStage = stageIdToStage(task.stageId)
        val mapStage = shuffleToMapStage(shuffleId)

        if (failedStage.latestInfo.attemptId != task.stageAttemptId) {
          logInfo(s"Ignoring fetch failure from $task as it's from $failedStage attempt" +
            s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
            s"(attempt ID ${failedStage.latestInfo.attemptId}) running")
        } else {

          // It is likely that we receive multiple FetchFailed for a single stage (because we have
          // multiple tasks running concurrently on different executors). In that case, it is
          // possible the fetch failure has already been handled by the scheduler.
          if (runningStages.contains(failedStage)) {
            logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
              s"due to a fetch failure from $mapStage (${mapStage.name})")
            markStageAsFinished(failedStage, Some(failureMessage))
          } else {
            logDebug(s"Received fetch failure from $task, but its from $failedStage which is no " +
              s"longer running")
          }

          if (disallowStageRetryForTest) {
            abortStage(failedStage, "Fetch failure will not retry stage due to testing config",
              None)
          } else if (failedStages.isEmpty) {
            // Don't schedule an event to resubmit failed stages if failed isn't empty, because
            // in that case the event will already have been scheduled.
            // TODO: Cancel running tasks in the stage
            logInfo(s"Resubmitting $mapStage (${mapStage.name}) and " +
              s"$failedStage (${failedStage.name}) due to fetch failure")
            messageScheduler.schedule(new Runnable {
              override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
            }, DAGScheduler.RESUBMIT_TIMEOUT, TimeUnit.MILLISECONDS)
          }
          failedStages += failedStage
          failedStages += mapStage
          // Mark the map whose fetch failed as broken in the map stage
          if (mapId != -1) {
            mapStage.removeOutputLoc(mapId, bmAddress)
            mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)
          }

          // TODO: mark the executor as failed only if there were lots of fetch failures on it
          if (bmAddress != null) {
            handleExecutorLost(bmAddress.executorId, fetchFailed = true, Some(task.epoch))
          }
        }

      case commitDenied: TaskCommitDenied =>
        // Do nothing here, left up to the TaskScheduler to decide how to handle denied commits

      case exceptionFailure: ExceptionFailure =>
        // Do nothing here, left up to the TaskScheduler to decide how to handle user failures

      case TaskResultLost =>
        // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case other =>
        // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
        // will abort the job.
    }
    submitWaitingStages()
  }

  /**
   * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
   *
   * We will also assume that we've lost all shuffle blocks associated with the executor if the
   * executor serves its own blocks (i.e., we're not using external shuffle) OR a FetchFailed
   * occurred, in which case we presume all shuffle data related to this executor to be lost.
   *
   * Optionally the epoch during which the failure was caught can be passed to avoid allowing
   * stray fetch failures from possibly retriggering the detection of a node as lost.
   */
  private[scheduler] def handleExecutorLost(
      execId: String,
      fetchFailed: Boolean,
      maybeEpoch: Option[Long] = None) {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    if (!failedEpoch.contains(execId) || failedEpoch(execId) < currentEpoch) {
      failedEpoch(execId) = currentEpoch
      logInfo("Executor lost: %s (epoch %d)".format(execId, currentEpoch))
      blockManagerMaster.removeExecutor(execId)

      if (!env.blockManager.externalShuffleServiceEnabled || fetchFailed) {
        // TODO: This will be really slow if we keep accumulating shuffle map stages
        for ((shuffleId, stage) <- shuffleToMapStage) {
          stage.removeOutputsOnExecutor(execId)
          val locs = stage.outputLocs.map(list => if (list.isEmpty) null else list.head)
          mapOutputTracker.registerMapOutputs(shuffleId, locs, changeEpoch = true)
        }
        if (shuffleToMapStage.isEmpty) {
          mapOutputTracker.incrementEpoch()
        }
        clearCacheLocs()
      }
    } else {
      logDebug("Additional executor lost message for " + execId +
               "(epoch " + currentEpoch + ")")
    }
    submitWaitingStages()
  }

  private[scheduler] def handleExecutorAdded(execId: String, host: String) {
    // remove from failedEpoch(execId) ?
    if (failedEpoch.contains(execId)) {
      logInfo("Host added was in lost list earlier: " + host)
      failedEpoch -= execId
    }
    submitWaitingStages()
  }

  private[scheduler] def handleStageCancellation(stageId: Int) {
    stageIdToStage.get(stageId) match {
      case Some(stage) =>
        val jobsThatUseStage: Array[Int] = stage.jobIds.toArray
        jobsThatUseStage.foreach { jobId =>
          handleJobCancellation(jobId, s"because Stage $stageId was cancelled")
        }
      case None =>
        logInfo("No active jobs to kill for Stage " + stageId)
    }
    submitWaitingStages()
  }

  private[scheduler] def handleJobCancellation(jobId: Int, reason: String = "") {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      failJobAndIndependentStages(
        jobIdToActiveJob(jobId), "Job %d cancelled %s".format(jobId, reason))
    }
    submitWaitingStages()
  }

  /**
   * Marks a stage as finished and removes it from the list of running stages.
   */
  private def markStageAsFinished(stage: Stage, errorMessage: Option[String] = None): Unit = {
    val serviceTime = stage.latestInfo.submissionTime match {
      case Some(t) => "%.03f".format((clock.getTimeMillis() - t) / 1000.0)
      case _ => "Unknown"
    }
    if (errorMessage.isEmpty) {
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      stage.latestInfo.completionTime = Some(clock.getTimeMillis())
    } else {
      stage.latestInfo.stageFailed(errorMessage.get)
      logInfo("%s (%s) failed in %s s".format(stage, stage.name, serviceTime))
    }
    outputCommitCoordinator.stageEnd(stage.id)
    listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
    runningStages -= stage
  }

  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   */
  private[scheduler] def abortStage(
      failedStage: Stage,
      reason: String,
      exception: Option[Throwable]): Unit = {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    val dependentJobs: Seq[ActiveJob] =
      activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
    failedStage.latestInfo.completionTime = Some(clock.getTimeMillis())
    for (job <- dependentJobs) {
      failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason", exception)
    }
    if (dependentJobs.isEmpty) {
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
  }

  /** Fails a job and all stages that are only used by that job, and cleans up relevant state. */
  private def failJobAndIndependentStages(
      job: ActiveJob,
      failureReason: String,
      exception: Option[Throwable] = None): Unit = {
    val error = new SparkException(failureReason, exception.getOrElse(null))
    var ableToCancelStages = true

    val shouldInterruptThread =
      if (job.properties == null) false
      else job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false").toBoolean

    // Cancel all independent, running stages.
    val stages = jobIdToStageIds(job.jobId)
    if (stages.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    }
    stages.foreach { stageId =>
      val jobsForStage: Option[HashSet[Int]] = stageIdToStage.get(stageId).map(_.jobIds)
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {
        logError(
          "Job %d not registered for stage %d even though that stage was registered for the job"
            .format(job.jobId, stageId))
      } else if (jobsForStage.get.size == 1) {
        if (!stageIdToStage.contains(stageId)) {
          logError(s"Missing Stage for stage with id $stageId")
        } else {
          // This is the only job that uses this stage, so fail the stage if it is running.
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {
            try { // cancelTasks will fail if a SchedulerBackend does not implement killTask
              taskScheduler.cancelTasks(stageId, shouldInterruptThread)
              markStageAsFinished(stage, Some(failureReason))
            } catch {
              case e: UnsupportedOperationException =>
                logInfo(s"Could not cancel tasks for stage $stageId", e)
              ableToCancelStages = false
            }
          }
        }
      }
    }

    if (ableToCancelStages) {
      job.listener.jobFailed(error)
      cleanupStateForJobAndIndependentStages(job)
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  /** Return true if one of stage's ancestors is target. */
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    if (stage == target) {
      return true
    }
    val visitedRdds = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              val mapStage = getShuffleMapStage(shufDep, stage.firstJobId)
              if (!mapStage.isAvailable) {
                waitingForVisit.push(mapStage.rdd)
              }  // Otherwise there's no need to follow the dependency back
            case narrowDep: NarrowDependency[_] =>
              waitingForVisit.push(narrowDep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    visitedRdds.contains(target.rdd)
  }

  /**
   * Gets the locality information associated with a partition of a particular RDD.
   *
   * This method is thread-safe and is called from both DAGScheduler and SparkContext.
   *
   * @param rdd whose partitions are to be looked at
   * @param partition to lookup locality information for
   * @return list of machines that are preferred by the partition
   */
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /**
   * Recursive implementation for getPreferredLocs.
   *
   * This method is thread-safe because it only accesses DAGScheduler state through thread-safe
   * methods (getCacheLocs()); please be careful when modifying this method, because any new
   * DAGScheduler state accessed by it may require additional synchronization.
   */
  /*
  * 计算每个task对应的partition的最佳位置
  * 就是从stage的最后一个RDD开始，找到哪一个rdd的partition是被cache了或checkpoint了，那么
  * task的最佳位置就是缓存的或者checkpoint的partition的位置，
  * 因为这样的话，task就在那个节点上执行，不需要计算之前的rdd了
  * */
  private def getPreferredLocsInternal(
      rdd: RDD[_],
      partition: Int,
      visited: HashSet[(RDD[_], Int)]): Seq[TaskLocation] = {
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    if (!visited.add((rdd, partition))) {
      // Nil has already been returned for previously visited partitions.
      return Nil
    }
    // If the partition is cached, return the cache locations
    val cached = getCacheLocs(rdd)(partition)
    if (cached.nonEmpty) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (rddPrefs.nonEmpty) {
      return rddPrefs.map(TaskLocation(_))
    }

    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }
      case _ =>
    }

    // If the RDD has shuffle dependencies and shuffle locality is enabled, pick locations that
    // have at least REDUCER_PREF_LOCS_FRACTION of data as preferred locations
    if (shuffleLocalityEnabled && rdd.partitions.length < SHUFFLE_PREF_REDUCE_THRESHOLD) {
      rdd.dependencies.foreach {
        case s: ShuffleDependency[_, _, _] =>
          if (s.rdd.partitions.length < SHUFFLE_PREF_MAP_THRESHOLD) {
            // Get the preferred map output locations for this reducer
            val topLocsForReducer = mapOutputTracker.getLocationsWithLargestOutputs(s.shuffleId,
              partition, rdd.partitions.length, REDUCER_PREF_LOCS_FRACTION)
            if (topLocsForReducer.nonEmpty) {
              return topLocsForReducer.get.map(loc => TaskLocation(loc.host, loc.executorId))
            }
          }
        case _ =>
      }
    }
    Nil
  }

  def stop() {
    logInfo("Stopping DAGScheduler")
    messageScheduler.shutdownNow()
    eventProcessLoop.stop()
    taskScheduler.stop()
  }

  // Start the event thread and register the metrics source at the end of the constructor
  env.metricsSystem.registerSource(metricsSource)
  eventProcessLoop.start()
}

private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

  /**
   * The main event loop of the DAG scheduler.
   */
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }

  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)

    case StageCancelled(stageId) =>
      dagScheduler.handleStageCancellation(stageId)

    case JobCancelled(jobId) =>
      dagScheduler.handleJobCancellation(jobId)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId) =>
      dagScheduler.handleExecutorLost(execId, fetchFailed = false)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion @ CompletionEvent(task, reason, _, _, taskInfo, taskMetrics) =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stop()
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object DAGScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200
}
