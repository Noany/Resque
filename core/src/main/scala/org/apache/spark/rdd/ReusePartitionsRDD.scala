package org.apache.spark.rdd

import java.util.List

import org.apache.spark.storage.{StorageLevel, RDDBlockId}
import org.apache.spark.{TaskContext, Partition, SparkEnv, SparkContext}
import tachyon.thrift.ClientFileInfo

import scala.collection.Iterator
import scala.reflect.ClassTag

/**
 * Created by zengdan on 15-10-15.
 */

class ReusePartitionsRDD[U: ClassTag, T: ClassTag](tachyonRdd: RDD[T],
                                         backupRDD: RDD[U], f: (TaskContext, Int, Iterator[T]) => Iterator[U])
  extends RDD[U](tachyonRdd.sparkContext, Nil){
  //this.persist(StorageLevel.OFF_HEAP)

  val tachyonPartitions = new Array[Partition](backupRDD.partitions.size)

  override def getPartitions: Array[Partition] = {
    val tP = tachyonRdd.partitions
    val ps = backupRDD.partitions
    var i = 0
    while (i < tP.size) {
      if (null != tP(i)) {
        ps(tP(i).index) = tP(i)
        tachyonPartitions(tP(i).index) = tP(i)
      }
      i += 1
    }
    ps
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    if (null == tachyonPartitions(split.index)) {
      backupRDD.preferredLocations(split)
    } else {
      tachyonRdd.preferredLocations(split)
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
    if (null == tachyonPartitions(split.index)) {
      backupRDD.compute(split, context)
    } else {
      f(context, split.index, tachyonRdd.compute(split, context))
    }
  }
}

