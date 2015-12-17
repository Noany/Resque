package org.apache.spark.storage

import java.io.IOException
import java.util
import akka.actor.ActorRef
import org.apache.spark.SparkEnv
import tachyon.TachyonURI
import tachyon.client.TachyonFS
import tachyon.conf.TachyonConf

import scala.collection.JavaConversions._

import com.google.common.base.Preconditions
import org.slf4j.LoggerFactory
import tachyon.worker.block.meta.{StorageTierView, BlockMeta, StorageDirView}
import tachyon.worker.block.{BlockStoreEventListenerBase, BlockMetadataManagerView, BlockStoreLocation}
import tachyon.worker.block.evictor.{EvictionPlan, Evictor}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by zengdan on 15-11-3.
 */
class ReuseEvictor {
  //extends BlockStoreEventListenerBase with Evictor
  /*

  val LOG = LoggerFactory.getLogger("");
  var mManagerView:BlockMetadataManagerView = null;
  var client: TachyonFS = _
  var qgworker: ActorRef = _
  var rootPath = SparkEnv.get.conf.get("spark.tachyonStore.global.baseDir",
    "/global_spark_tachyon")
  lazy val blockInfos = getLocalBlocksInfos()

  def this(view: BlockMetadataManagerView) = {
    this()
    mManagerView = Preconditions.checkNotNull(view);
  }

  def setQGWorker(worker: ActorRef) = {
    qgworker = worker
  }

  def setTachyonFS(fs: TachyonFS): Unit ={
    client = fs
  }

  override def freeSpaceWithView(availableBytes: Long, location: BlockStoreLocation,
                        view: BlockMetadataManagerView): EvictionPlan = {
    mManagerView = view;
    return freeSpace(availableBytes, location);
  }


  @throws(classOf[IOException])
  def freeSpace(availableBytes: Long, location: BlockStoreLocation):EvictionPlan = {
    //get the blocks to evict
    //traverse the blocks of the files in the

    // 1. Select a StorageDirView that has enough capacity for required bytes.
    val pendingBytesInDir = new util.HashMap[StorageDirView, Long]();
    var selectedDirView:StorageDirView = null;
    var victimBlocks: ArrayBuffer[(BlockMeta, Double)] = null;
    if (location.equals(BlockStoreLocation.anyTier())) {
      (selectedDirView, victimBlocks) = selectDirToEvictBlocksFromAnyTier(availableBytes, Double.MaxValue, pendingBytesInDir);
    } else {
      val tierAlias = location.tierAlias();
      val tierView:StorageTierView = mManagerView.getTierView(tierAlias);
      if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
        (selectedDirView, victimBlocks) = selectDirToEvictBlocksFromTier(tierView, availableBytes, Double.MaxValue, pendingBytesInDir);
      } else {
        val dirIndex = location.dir();
        val dir:StorageDirView = tierView.getDirView(dirIndex);
        victimBlocks = canEvictBlocksFromDir(dir, availableBytes, Double.MaxValue, pendingBytesInDir)
        if (!victimBlocks.isEmpty) {
          selectedDirView = dir
        }

      }
    }
    if (selectedDirView == null) {
      LOG.error("Failed to freeSpace: No StorageDirView has enough capacity of {} bytes",
        availableBytes);
      return null;
    }

    // 2. Check if the selected StorageDirView already has enough space.
    val toTransfer =
      new java.util.ArrayList[Pair[Long, BlockStoreLocation]]()
    val toEvict:util.List[Long] = new util.ArrayList[Long]()
    var bytesAvailableInDir = selectedDirView.getAvailableBytes();
    if (bytesAvailableInDir >= availableBytes) {
      // No need to evict anything, return an eviction plan with empty instructions.
      return new EvictionPlan(bufferAsJavaList(toTransfer), bufferAsJavaList(toEvict));
    }

    // 4. Make best effort to transfer victim blocks to lower tiers rather than evict them.

    transferBlocks(victimBlocks, toEvict, toTransfer, pendingBytesInDir)
    return new EvictionPlan(bufferAsJavaList(toTransfer), bufferAsJavaList(toEvict));
  }

  def transferBlocks(victimBlocks: ArrayBuffer[(BlockMeta, Double)], toEvict: util.List[Long],
                      toTransfer: util.List[Pair[Long, BlockStoreLocation]],
                     pendingBytesInDir: java.util.Map[StorageDirView, Long]):Unit = {
    val nextVictims = ArrayBuffer[(BlockMeta, Double)]()
    for (blockInfo: (BlockMeta, Double) <- victimBlocks) {
      // TODO: should avoid calling getParentDir
      val block = blockInfo._1
      val fromTierAlias = block.getParentDir().getParentTier().getTierAlias();
      val toTiers = mManagerView.getTierViewsBelow(fromTierAlias);
      //无级连替换，下一层没有足够空间，直接evict，否则transfer
      val (toDir, curVictims) = selectDirToTransferBlock(blockInfo, toTiers, pendingBytesInDir);
      if (toDir == null) {
        // Not possible to transfer
        toEvict.add(block.getBlockId());
      } else {
        val toTier = toDir.getParentTierView();
        toTransfer.add(new Pair[Long, BlockStoreLocation](block.getBlockId(),
          new BlockStoreLocation(toTier.getTierViewAlias(), toTier.getTierViewLevel(), toDir
            .getDirViewIndex())));
        if (pendingBytesInDir.containsKey(toDir)) {
          pendingBytesInDir.put(toDir, pendingBytesInDir.get(toDir) + block.getBlockSize());
        } else {
          pendingBytesInDir.put(toDir, block.getBlockSize());
        }
        if(!curVictims.isEmpty){
          nextVictims ++= curVictims
        }
      }
    }
    transferBlocks(victimBlocks, toEvict, toTransfer, pendingBytesInDir)
  }

  //To do: Synchronized
  //Now assume that the blocks newly entered in needn't to be evicted
  def getLocalBlocksInfos(): Map[(Int, Int), ArrayBuffer[(BlockMeta, Double)]] = {
    val files = client.listStatus(new TachyonURI(rootPath))
    val candidateBlocks = Map[(Int, Int),ArrayBuffer[(BlockMeta, Double)]]()
    //find out the blocks that belongs to the same tier level and the same local machine
    for(file <- files) {
      val curCandidates = ArrayBuffer[BlockMeta]()
      //get file benefit
      for (blockFile <- client.listStatus(new TachyonURI(file.getPath))) {
        for (id <- blockFile.getBlockIds) {
          val meta = try {
            mManagerView.getBlockMeta(id)
          } catch {
            case e: IOException => null
            case _ => null
          }
          if (meta != null) {
            //val location = meta.getBlockLocation
            //if (location.tierLevel() == level && location.dir() == dirView.getDirViewIndex) {
            curCandidates += meta
          }
        }
      }
      if(!curCandidates.isEmpty) {
        val fromIndex = file.getPath.lastIndexOf('/')
        val benefit = QGWorker.getBenefit(file.getPath.substring(fromIndex + 1).toInt, SparkEnv.get.conf, qgworker)
        //没有需要存的block的信息，无法排除掉与此block同属一个rdd的block,无法得知其benefit，暂且优先存储
        curCandidates.foreach{blockMeta: BlockMeta =>
          val location = blockMeta.getBlockLocation
          val key = (location.tierLevel(), location.dir())
          val buffers = candidateBlocks.get(key).getOrElse(ArrayBuffer[(BlockMeta, Double)]())
          buffers += ((blockMeta, benefit))
          candidateBlocks.put(key, buffers)
        }
      }
    }

    for((key, value) <- candidateBlocks){
      value.sortWith((x, y) => x._2 < y._2)
    }
    candidateBlocks
  }

  def canEvictBlocksFromDir(dirView: StorageDirView, availableBytes: Long, benefit: Double,
                            pendingBytesInDir: java.util.Map[StorageDirView, Long]): ArrayBuffer[(BlockMeta, Double)] = {
    //modify dirView.getEvitableBytes
    val blocks = blockInfos.get((dirView.getParentTierView.getTierViewLevel, dirView.getDirViewIndex))
    val victims: ArrayBuffer[(BlockMeta, Double)] = ArrayBuffer[(BlockMeta, Double)]()
    if(blocks.isDefined){
      var pendingBytes = 0L;
      if (pendingBytesInDir.containsKey(dirView)) {
        pendingBytes = pendingBytesInDir.get(dirView);
      }
      var requiredBytes = availableBytes - (dirView.getAvailableBytes - pendingBytes)
      var i = 0
      while(i < blocks.get.size && requiredBytes > 0 && blocks.get(i)._2 <= benefit){
        requiredBytes -= blocks.get(i)._1.getBlockSize
        victims += ((blocks.get(i)._1, blocks.get(i)._2))
        i += 1
      }
      if(requiredBytes > 0){
        victims.clear()
      }
    }
    victims
  }

  def selectDirToEvictBlocksFromAnyTier(availableBytes: Long, benefit: Double,
         pendingBytesInDir: java.util.Map[StorageDirView, Long]):(StorageDirView, ArrayBuffer[(BlockMeta, Double)]) = {
    for (tierView: StorageTierView <- mManagerView.getTierViews()) {
      for (dirView: StorageDirView <- tierView.getDirViews()) {
        var pendingBytes = 0L;
        if (pendingBytesInDir.containsKey(dirView)) {
          pendingBytes = pendingBytesInDir.get(dirView);
        }
        if(dirView.getAvailableBytes - pendingBytes >= availableBytes){
          return (dirView, ArrayBuffer[(BlockMeta, Double)]())
        }
        val victims = canEvictBlocksFromDir(dirView, availableBytes, benefit, pendingBytesInDir)
        if (!victims.isEmpty) {
          return (dirView, victims);
        }
      }
    }
    return (null, null);
  }

  def selectDirToEvictBlocksFromTier(tierView:StorageTierView, availableBytes: Long, benefit: Double,
        pendingBytesInDir: java.util.Map[StorageDirView, Long]):(StorageDirView, ArrayBuffer[(BlockMeta, Double)]) = {
    for (dirView: StorageDirView  <- tierView.getDirViews()) {
      var pendingBytes = 0L;
      if (pendingBytesInDir.containsKey(dirView)) {
        pendingBytes = pendingBytesInDir.get(dirView);
      }
      if(dirView.getAvailableBytes - pendingBytes >= availableBytes){
        return (dirView, ArrayBuffer[(BlockMeta, Double)]())
      }
      val victims = canEvictBlocksFromDir(dirView, availableBytes, benefit, pendingBytesInDir)
      if (!victims.isEmpty) {
        return (dirView, victims);
      }
    }
    return (null, null);
  }

  def selectDirToTransferBlock(blockInfo: (BlockMeta, Double), toTiers: util.List[StorageTierView],
       pendingBytesInDir: java.util.Map[StorageDirView, Long]):(StorageDirView, ArrayBuffer[(BlockMeta, Double)]) = {

    for (toTier <- toTiers) {
      for (toDir:StorageDirView <- toTier.getDirViews()) {
        var pendingBytes = 0L;
        if (pendingBytesInDir.containsKey(toDir)) {
          pendingBytes = pendingBytesInDir.get(toDir);
        }
        if(toDir.getAvailableBytes - pendingBytes >= blockInfo._1.getBlockSize()){
          return (toDir, ArrayBuffer[(BlockMeta, Double)]())
        }
        val victims = canEvictBlocksFromDir(toDir, blockInfo._1.getBlockSize(), blockInfo._2, pendingBytesInDir)
        if (!victims.isEmpty) {
          return (toDir, victims);
        }
      }
    }
    return null;
  }
  */
}

