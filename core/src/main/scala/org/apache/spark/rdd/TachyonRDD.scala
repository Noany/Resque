package org.apache.spark.rdd

import java.util.List

import org.apache.spark.storage.RDDBlockId
import org.apache.spark.{TaskContext, Partition, SparkEnv, SparkContext}
import tachyon.thrift.ClientFileInfo

import scala.reflect.ClassTag

/**
 * Created by zengdan on 15-10-15.
 */
private[spark] class TachyonPartition(idx: Int)
  extends Partition {
  override val index: Int = idx
}

class TachyonRDD [T: ClassTag](sc: SparkContext, operatorId: Int)
  extends ExternalStoreRDD[T](sc, operatorId){


  override def getPartitions: Array[Partition] = {
    val files: List[ClientFileInfo] = externalBlockStore.listStatus(operatorId)
    val ps = new Array[Partition](files.size())
    var i = 0
    while(i < files.size()){
      ps(i) = new TachyonPartition(files.get(i).name.split("_").last.toInt)
      i += 1
    }
    ps
  }

  override def compute(split: Partition, context: TaskContext) = {
    val fileName = "operator_" + operatorId + "_" + split.index
    logInfo("Input Split: " + fileName)
    externalBlockStore.getValues(new RDDBlockId(operatorId, split.index, Some(operatorId)))
      .getOrElse(Iterator.empty).asInstanceOf[Iterator[T]]
  }
}
