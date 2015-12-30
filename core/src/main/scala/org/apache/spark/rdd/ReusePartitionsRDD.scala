package org.apache.spark.rdd

import java.util
import org.apache.spark._

import scala.collection.Iterator
import scala.collection.mutable.{Stack, HashSet}
import scala.reflect.ClassTag

/**
 * Created by zengdan on 15-10-15.
 */

class ReusePartitionsRDD[U: ClassTag, T: ClassTag](tachyonRdd: RDD[T],
                                         backupRDD: RDD[U], f: (TaskContext, Int, Iterator[T]) => Iterator[U])
  extends RDD[U](tachyonRdd.sparkContext, Nil){
  //this.persist(StorageLevel.OFF_HEAP)

  //val tachyonPartitions = new Array[Partition](backupRDD.partitions.size)
  val tachyonPartitions = new Array[Boolean](backupRDD.partitions.size)
  var recomputeTotal = false

  override def getPartitions: Array[Partition] = {
    val tP = tachyonRdd.partitions
    val ps = backupRDD.partitions
    var i = 0

    while (!recomputeTotal && i < tP.size) {
      if (null != tP(i)) {
        ps(tP(i).index) = tP(i)
        tachyonPartitions(tP(i).index) = true
      } else {
        //identify if recomputation needs shuffle
        if (needShuffle) {
          util.Arrays.fill(tachyonPartitions, false)
          recomputeTotal = true
        }
      }
      i += 1
    }
    if(recomputeTotal) backupRDD.partitions
    else ps
  }

  private def needShuffle(): Boolean = {
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def hasShuffle(r: RDD[_]):Boolean = {
      if (!visited(r)) {
        visited += r
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of partitions is unknown
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              return true
            case _ =>
              waitingForVisit.push(dep.rdd)
          }
        }
      }
      false
    }
    waitingForVisit.push(backupRDD)
    while (waitingForVisit.nonEmpty) {
      if (hasShuffle(waitingForVisit.pop())) {
        return true
      }
    }
    false
  }

  override def getDependencies: Seq[Dependency[_]] = {
    if (recomputeTotal) List(new OneToOneDependency(backupRDD))
    else Nil
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    if (tachyonPartitions(split.index)) {
      tachyonRdd.preferredLocations(split)
    } else {
      backupRDD.preferredLocations(split)
    }
    /*
    if (split.isInstanceOf[TachyonPartition]) {
      tachyonRdd.preferredLocations(split)
    } else {
      backupRDD.preferredLocations(split)
    }
    */
  }

  override def compute(split: Partition, context: TaskContext) = {
    /*
    if (split.isInstanceOf[TachyonPartition]) {
      f(context, split.index, tachyonRdd.compute(split, context))
    } else {
      backupRDD.compute(split, context)
    }
    */
    if (tachyonPartitions(split.index)) {
      f(context, split.index, tachyonRdd.compute(split, context))
    } else {
      backupRDD.compute(split, context)
    }
  }
}

