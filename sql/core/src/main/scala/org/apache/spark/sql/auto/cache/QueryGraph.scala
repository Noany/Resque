package org.apache.spark.sql.auto.cache

/**
 * Created by zengdan on 15-3-13.
 */

//import java.util.HashMap

import java.io.IOException
import java.nio.{ByteOrder, ByteBuffer}
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.auto.cache.QGMaster._
import org.apache.spark.sql.auto.cache.QGUtils.{NodeDesc, PlanUpdate}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.{RangePartitioning, HashPartitioning}
import org.apache.spark.sql.execution.QNodeRef
import org.apache.spark.sql.columnar.{InMemoryColumnarTableScan, InMemoryRelation}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.TungstenAggregate
import org.apache.spark.storage.{ExternalBlockStore, TachyonBlockManager}
import org.apache.spark.util.SignalLogger
import org.apache.spark.{Logging, SparkConf, SparkContext}
import tachyon.TachyonURI
import tachyon.conf.TachyonConf
import tachyon.util.CommonUtils

import scala.collection.mutable.{ArrayBuffer, Map, HashMap}

import tachyon.client.{WriteType, OutStream, TachyonFile, TachyonFS}



object QueryNode{
  var counter = new AtomicInteger(0)
  val cache_threshold = 0  //TO CALIBRATE
}

class QueryNode(plan: SparkPlan) {
  //val parents = new ConcurrentHashMap[Int, ArrayBuffer[QueryNode]]()  //hashcode -> parent
  val parents = new ArrayBuffer[QueryNode]()
  var id: Int = QueryNode.counter.getAndIncrement
  //var cache = false

  val stats: Array[Long] = new Array[Long](3)
  //ref,time, size

  var lastAccess: Long = System.currentTimeMillis()
  var cached: Boolean = false   //whether has data in tachyon

  var schema: Option[Seq[Attribute]] = None

  def getPlan: SparkPlan = plan

  override def toString() = {
    val planClassName = plan.getClass.getName
    val index = planClassName.lastIndexOf(".")

    "id: " + id + " statistics: " + stats(0) + " " + stats(1) + " " + stats(2) +
      " " + planClassName.substring(index+1) + " cached: " + cached
  }

}

class QueryGraph(conf: SparkConf){

  /*
   * TODO: parents synchronized
   */
  val root = new QueryNode(null)
  val nodes = new HashMap[Int, QueryNode]() //id -> node

  var maxBenefit = Double.MinValue
  var maxPlan: ArrayBuffer[SparkPlan] = new ArrayBuffer[SparkPlan]()

  val memThreshold = conf.get("spark.sql.reuse.memory.benefit.threshold", "0").toInt
  val diskThreshold = conf.get("spark.sql.reuse.disk.benefit.threshold", "0").toInt

  var client: tachyon.client.TachyonFS = _

  val master = conf.get(ExternalBlockStore.MASTER_URL, "tachyon://localhost:19998")
  client = if (master != null && master != "") {
    val tachyonConf = new TachyonConf()
    tachyonConf.set("tachyon.user.quota.unit.bytes", "536870912")
    TachyonFS.get(new TachyonURI(master), tachyonConf)
  } else {
    null
  }
  // original implementation call System.exit, we change it to run without extblkstore support
  if (client == null) {
    throw new IOException("Failed to connect to the Tachyon as the master " +
      "address is not configured")
  }

  val rootPath = conf.get("spark.tachyonStore.global.baseDir" , "/global_spark_tachyon")

  /*
   * TODO: cut Graph to save space
   */
  def cutGraph() {

  }

  def addNode(curChild:ArrayBuffer[QueryNode],
              plan: SparkPlan,
              refs: HashMap[Int, QNodeRef]): Unit = {
    val newNode = new QueryNode(plan)
    curChild.foreach(_.parents.append(newNode))
    nodes.put(newNode.id,newNode)
    plan.nodeRef = Some(QNodeRef(newNode.id, false, true, false, -1))
    refs.put(plan.id, plan.nodeRef.get)
    newNode.stats(0) = 1
  }


  def planRewritten(plan: SparkPlan): PlanUpdate = {
    maxBenefit = Double.MinValue
    maxPlan.clear()
    val refs = new HashMap[Int, QNodeRef]()
    val varNodes = new HashMap[Int, ArrayBuffer[NodeDesc]]()
    matchPlan(plan, refs, varNodes, maxPlan)
    for(mPlan <- maxPlan) {
      val mNode = nodes.get(mPlan.nodeRef.get.id).get
      if(!mNode.cached && getBenefit(mNode) > QueryNode.cache_threshold) {
        //concurrent control
        //Anytime there doesn't exist two process writing the same file
        mPlan.nodeRef.get.cache = true
        mNode.cached = true
      }
    }

    //println("========maxNodes========")
    for(mPlan <- maxPlan){
      //println(nodes.get(mPlan.nodeRef.get.id))
    }
    //println("========maxNodes========")
    PlanUpdate(refs, varNodes)
  }

  /*
  def update(node: QueryNode, plan: SparkPlan)= {
    //没有统计信息的暂不参与计算
    if(node.stats(2) > 0){
      val benefit = node.stats(0)*node.stats(1)*1.0/node.stats(2)
      if(benefit > maxBenefit){
        maxBenefit = benefit
        if(maxNode.size < 1) {
          maxNode.append(node)
          maxPlan.append(plan)
        }else {
          maxNode(0) = node
          maxPlan(0) = plan
        }
      }
    }
  }
  */

  def getBenefit(node: QueryNode): Double = if(node.stats(2) > 0){
    node.stats(0)*node.stats(1)*1.0/node.stats(2)
  }else{
    0.0
  }

  def getBenefit(node: QueryNode, mem: Boolean): Double = if(node.stats(2) > 0){
    node.stats(0)*node.stats(1)*1.0/node.stats(2)
  }else{
    0.0
  }

  def update(node: QueryNode, plan: SparkPlan,
             maxPlans: ArrayBuffer[SparkPlan])= {
    node.stats(0) += 1
    node.lastAccess = System.currentTimeMillis()
    //reuse stored data
    //zengdan test failure
    if(node.cached) {
      plan.nodeRef.get.reuse = true
    }
    //没有统计信息的暂不参与计算
    if(node.stats(2) > 0){
      val benefit = node.stats(0)*node.stats(1)*1.0/node.stats(2)
      if(maxPlans.size == 0){
        maxPlans.append(plan)
      }else{
        var i = 0
        var remove = false
        while(i < maxPlans.size){
          val curNode = nodes.get(maxPlans(i).nodeRef.get.id).get
          if(getBenefit(curNode) <= benefit){
            maxPlans.remove(i)
            remove = true
          }else{
            i += 1
          }
        }
        if(remove){
          maxPlans.append(plan)
        }
      }
    }
  }

  def matchPlan(plan: SparkPlan, refs: HashMap[Int, QNodeRef],
                varNodes: HashMap[Int, ArrayBuffer[NodeDesc]],
                maxPlans: ArrayBuffer[SparkPlan]):Unit = {

    if((plan.children == null || plan.children.length <= 0) &&
      !plan.isInstanceOf[InMemoryColumnarTableScan]){
      for (leave <- root.parents) {
        if (leave.getPlan.operatorMatch(plan)) {
          //leave.stats(0) += 1
          plan.nodeRef = Some(QNodeRef(leave.id, false, false, false, leave.stats(1)))
          update(leave, plan, maxPlans)
          refs.put(plan.id, plan.nodeRef.get)
          return
        }
      }
      return addNode(ArrayBuffer(root), plan, refs)
    }

    //ensure all children matches

    val children = new ArrayBuffer[QueryNode]()
    if(plan.isInstanceOf[InMemoryColumnarTableScan]){
      val child = plan.asInstanceOf[InMemoryColumnarTableScan].relation.child
      if(!child.nodeRef.isDefined) {
        matchPlan(child, refs, varNodes, maxPlans)
      }else{
        update(nodes.get(child.nodeRef.get.id).get, child, maxPlans)
      }
      children.append(nodes.get(child.nodeRef.get.id).get)
    } else if (plan.isInstanceOf[TungstenAggregate] && plan.children.size == 1 &&
      plan.children(0).isInstanceOf[TungstenAggregate] &&
      plan.children(0).asInstanceOf[TungstenAggregate].resultExpressions.isEmpty) {
      val branchMaxPlans = new Array[ArrayBuffer[SparkPlan]](plan.children(0).children.length)
      var i = 0
      while (i < plan.children(0).children.length) {
        branchMaxPlans(i) = new ArrayBuffer[SparkPlan]()
        val curChild = plan.children(0).children(i)
        matchPlan(curChild, refs, varNodes, branchMaxPlans(i))
        val curNode = nodes.get(curChild.nodeRef.get.id).get
        children.append(curNode)
        i += 1
      }

      maxPlans ++=
        branchMaxPlans.foldLeft(new ArrayBuffer[SparkPlan]())((buffer, i) => {
          i.foreach(buffer.append(_)); buffer
        })

    } else {
      val branchMaxPlans = new Array[ArrayBuffer[SparkPlan]](plan.children.length)
      var i = 0
      while (i < plan.children.length) {
        branchMaxPlans(i) = new ArrayBuffer[SparkPlan]()
        val curChild = plan.children(i)
        matchPlan(curChild, refs, varNodes, branchMaxPlans(i))
        val curNode = nodes.get(curChild.nodeRef.get.id).get
        children.append(curNode)
        i += 1
      }

      maxPlans ++=
        branchMaxPlans.foldLeft(new ArrayBuffer[SparkPlan]())((buffer, i) => {
          i.foreach(buffer.append(_)); buffer
        })
    }

    for (candidate <- children(0).parents) {

      if (candidate.getPlan.operatorMatch(plan)) {
        if((children.length == 1) ||
          (children.length > 1 && !children.exists(!_.parents.contains((candidate))))) {
          //candidate.stats(0) += 1
          plan.nodeRef = Some(QNodeRef(candidate.id, false, false, false, candidate.stats(1)))
          update(candidate, plan, maxPlans)
          refs.put(plan.id, plan.nodeRef.get)
          return
        }
      }
    }

    ///*
    //subsumption relationship
    subsumptionMatch(plan, children(0), varNodes)
    //*/

    //exchange reuse
    if(plan.isInstanceOf[Exchange]){
      var found = false
      var index = 0
      val curPlan = plan.asInstanceOf[Exchange]
      while(index < children(0).parents.length && !found){
        val candidate = children(0).parents(index)
        if (candidate.getPlan.isInstanceOf[Exchange]) {
          val canPlan = candidate.getPlan.asInstanceOf[Exchange]
          if (canPlan.newPartitioning.reuseMatch(curPlan.newPartitioning)) {
            //only handle different partition number now
            //TODO: handle different partition key
            if (TachyonBlockManager.checkOperatorFileExists(candidate.id)) {
              val buffers = varNodes.get(plan.id).getOrElse(new ArrayBuffer[NodeDesc]())
              val partition = curPlan.newPartitioning match {
                case HashPartitioning(exprs, _) => Some(HashPartitioning(exprs, canPlan.newPartitioning.numPartitions))
                case RangePartitioning(exprs, _) => Some(RangePartitioning(exprs, canPlan.newPartitioning.numPartitions))
                case _ => None
              }
              if (partition.isDefined) {
                buffers.append(NodeDesc(QNodeRef(candidate.id, false, false, true, candidate.stats(1)), partition.get))
                varNodes.put(plan.id, buffers)
                found = true
              }
            }
          }
        }
        index += 1
      }
    }
    //*/

    return addNode(children, plan, refs)
  }

  ///*
  def subsumptionMatch(plan: SparkPlan, child: QueryNode, varNodes: HashMap[Int, ArrayBuffer[NodeDesc]]): Unit = {
    plan match{

      case Filter(_, _) =>

      for(candidate <- child.parents){
        if(candidate.getPlan.isInstanceOf[Filter]){
          val candidateExpr = candidate.getPlan.transformedExpressions
          val planExpr = plan.transformedExpressions


        }
      }

      case Project(_, _) =>
        var found = false
        var index = 0


        while(index < child.parents.length && !found){
          val candidate = child.parents(index)
          if(candidate.getPlan.isInstanceOf[Project]){
            val candidateExpr = candidate.getPlan.transformedExpressions.map(_.treeStringByName)
            val planExpr = plan.transformedExpressions.map(_.treeStringByName)
            if(planExpr.filter(!candidateExpr.contains(_)).size == 0 &&
              TachyonBlockManager.checkOperatorFileExists(candidate.id)){
              val canPlan = candidate.getPlan.asInstanceOf[Project]
              val curLists = plan.asInstanceOf[Project].projectList.map(_.transformExpression())
              //To do: Optimize
              val newList = canPlan.projectList.map{x =>
                var canExpr = x.transformExpression()
                var i = 0
                var flag = true
                while(i < curLists.length && flag){
                  if(canExpr.compareTree(curLists(i)) == 0){
                    canExpr = curLists(i)
                    flag = false
                  }
                  i += 1
                }
                canExpr
              }

              val buffers = varNodes.get(plan.id).getOrElse(new ArrayBuffer[NodeDesc]())
              buffers.append(NodeDesc(QNodeRef(candidate.id, false, false, true, candidate.stats(1)), newList))
              varNodes.put(plan.id, buffers)

              //val newProject = new Project(canPlan.projectList, plan.children(0))
              //newProject.nodeRef = Some(QNodeRef(candidate.id, false, false, true))
              //plan.withNewChildren(Seq(newProject))
              found = true
            }
          }
          index += 1
        }

      case TungstenProject(_, _) =>
        var found = false
        var index = 0

        while(index < child.parents.length && !found){
          val candidate = child.parents(index)
          if(candidate.getPlan.isInstanceOf[TungstenProject]){
            val candidateExpr = candidate.getPlan.transformedExpressions.map(_.treeStringByName)
            val planExpr = plan.transformedExpressions.map(_.treeStringByName)
            if(planExpr.filter(!candidateExpr.contains(_)).size == 0 &&
              TachyonBlockManager.checkOperatorFileExists(candidate.id)){
              val canPlan = candidate.getPlan.asInstanceOf[TungstenProject]
              val curLists = plan.asInstanceOf[TungstenProject].projectList.map(_.transformExpression())
              //To do: Optimize
              val newList = canPlan.projectList.map{x =>
                var canExpr = x.transformExpression()
                var i = 0
                var flag = true
                while(i < curLists.length && flag){
                  if(canExpr.compareTree(curLists(i)) == 0){
                    canExpr = curLists(i)   //replace added project expression with current plan expression to avoid bound error
                    flag = false
                  }
                  i += 1
                }
                canExpr
              }

              val buffers = varNodes.get(plan.id).getOrElse(new ArrayBuffer[NodeDesc]())
              buffers.append(NodeDesc(QNodeRef(candidate.id, false, false, true, candidate.stats(1)), newList))
              varNodes.put(plan.id, buffers)

              //val newProject = new Project(canPlan.projectList, plan.children(0))
              //newProject.nodeRef = Some(QNodeRef(candidate.id, false, false, true))
              //plan.withNewChildren(Seq(newProject))
              found = true
            }
          }
          index += 1
        }

      case _ =>
    }
  }
  //*/

  def saveSchema(output: Seq[Attribute], id: Int): Unit ={
    val refNode = nodes.get(id)
    if(refNode.isDefined && refNode.get.cached){
      refNode.get.schema = Some(output)
    }
  }

  def getSchema(id: Int): Seq[Attribute] ={
    val refNode = nodes.get(id)
    if(refNode.isDefined && refNode.get.schema.isDefined){
      refNode.get.schema.get
    }else{
      Nil
    }
  }

  def cacheFailed(operatorId: Int){
    val nd = nodes.get(operatorId)
    if(nd.isDefined){
      nd.get.cached = false
    }
  }

  def updateStatistics(stats: Map[Int, Array[Long]]) = {
    for((key, value) <- stats){
      val refNode = nodes.get(key)
      if(refNode.isDefined){
        refNode.get.stats(1) = value(0)  //update time
        refNode.get.stats(2) = value(1)  //update size
      }
    }

    ///*
    var cachedBenefits = scala.collection.Map[String, java.lang.Double]()
    for ((id, node) <- nodes) {
      val path = rootPath + "/" + id
      if (node.cached ) {
        if(client.exist(new TachyonURI(path)))
          cachedBenefits += (path -> getBenefit(node))
      }
    }
    //*/

    import scala.collection.JavaConversions._

    if (!cachedBenefits.isEmpty) {
      client.qgmaster_setBenefit(mapAsJavaMap(cachedBenefits))
    }
    //QueryGraph.printResult(this)
  }

  def changeToMemory(id: Int) : Boolean = {
    if(nodes.get(id).isDefined && getBenefit(nodes.get(id).get, true) > memThreshold){
      true
    }else{
      false
    }
  }

  def getBenefit(id: Int) : Double = {
    if(nodes.get(id).isDefined){
      getBenefit(nodes.get(id).get)
    }else{
      throw new Exception("Get the benefit of non-exisiting node")
    }
  }
}

object  QueryGraph extends Logging{
  var qg = new QueryGraph(new SparkConf)

  def main(args: Array[String]) {

    val tachyonClient = qg.client
    val path1: TachyonURI = new TachyonURI("/global_spark_tachyon/1/operator_1_0")
    if (!tachyonClient.exist(path1)) {
      createFile(tachyonClient, path1)
    }
    writeFile(tachyonClient, path1, 1, 2.0)
    //tachyonClient.qgmaster_setBenefit()

    var cachedBenefits = scala.collection.Map[String, java.lang.Double]()
    val rootPath = "/global_spark_tachyon/"
    cachedBenefits += ((rootPath + "1") -> 2.0)

    import scala.collection.JavaConversions._
    tachyonClient.qgmaster_setBenefit(mapAsJavaMap(cachedBenefits))
    Thread.sleep(20000)


    val path2: TachyonURI = new TachyonURI("/global_spark_tachyon/2/operator_2_0")
    if (!tachyonClient.exist(path2)) {
      createFile(tachyonClient, path2)
    }
    writeFile(tachyonClient, path2, 2, 4.0)
    cachedBenefits += ((rootPath + "2") -> 4.0)
    tachyonClient.qgmaster_setBenefit(mapAsJavaMap(cachedBenefits))
    Thread.sleep(20000)

    //*/
    val path3: TachyonURI = new TachyonURI("/global_spark_tachyon/3/operator_3_0")
    if (!tachyonClient.exist(path3)) {
      createFile(tachyonClient, path3)
    }
    writeFile(tachyonClient, path3, 3, 3.0)
    cachedBenefits += ((rootPath + "3") -> 3.0)
    tachyonClient.qgmaster_setBenefit(mapAsJavaMap(cachedBenefits))
  }

  @throws(classOf[IOException])
  private def createFile(tachyonClient: TachyonFS, path: TachyonURI) {
    val startTimeMs: Long = CommonUtils.getCurrentMs
    val fileId: Int = tachyonClient.createFile(path)
  }

  @throws(classOf[IOException])
  private def writeFile(tachyonClient: TachyonFS, path: TachyonURI, id: Int, benefit: Double) {
    val buf: ByteBuffer = ByteBuffer.allocate(20 * 4)
    buf.order(ByteOrder.nativeOrder)
    for (k <- 0 to 19) {
      buf.putInt(k)
    }

    buf.flip
    buf.flip
    val startTimeMs: Long = CommonUtils.getCurrentMs
    val file: TachyonFile = tachyonClient.getFile(path)
    val os: OutStream = file.getOutStream(WriteType.TRY_CACHE, buf.array.length, id, 0, benefit)
    os.write(buf.array)
    os.close
  }

  def printResult(graph: QueryGraph){
    ///*
    println("=====Parents=====")
    graph.root.parents.foreach(println)


    println("=====Nodes=====")
    for((key, value) <- graph.nodes) {
      print(s"${key} ")
      print(s"${value} ")
      println()
    }

    println("===============")
    //*/

  }

}
