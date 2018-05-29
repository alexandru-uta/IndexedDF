package indexeddataframe.logical

import indexeddataframe.execution.IndexedOperatorExec
import indexeddataframe.{IRDD, Utils}
import org.apache.spark.sql.InMemoryRelationMatcher
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import indexeddataframe.logical.IndexedJoin

import scala.collection.concurrent.TrieMap

object IndexLocalRelation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case CreateIndex(colNo, LocalRelation(output, data)) =>
      IndexedLocalRelation(output, data)
  }
}

/**
  * set of rules to be applied for Indexed Data Frames
  */
object ConvertToIndexedOperators extends Rule[LogicalPlan] {

  /**
    * we need to keep track of which indexed data has been cached, much like Spark SQL's [CacheManager]
    */
  private val cachedPlan: TrieMap[SparkPlan, IRDD] = new TrieMap[SparkPlan, IRDD]

  /**
    * check if a physical plan has already been cached; if so, return it, otherwise cache it
    * @param plan
    * @return
    */
  private def getIfCached(plan: SparkPlan): IRDD = {
    val result = cachedPlan.get(plan)
    if (result == None) {
      val executedPlan = Utils.ensureCached(plan.asInstanceOf[IndexedOperatorExec].executeIndexed())
      cachedPlan.put(plan, executedPlan)
      executedPlan
    } else {
      result.get
    }
  }

  /**
    * check if a logical plan is constructed with indexed operators
    * @param plan
    * @return
    */
  def isIndexed(plan: LogicalPlan): Boolean = {
    plan.find {
      case _: IndexedOperator => true
      case _ => false
    }.nonEmpty
  }

  /**
    * check if a physical plan is constructed with indexed operators
    * @param plan
    * @return
    */
  def isIndexed(plan: SparkPlan): Boolean = {
    plan.find {
      case _: IndexedOperatorExec => true
      case _ => false
    }.nonEmpty
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {

    /**
      * replace Spark's default .cache() method with our own cache implementation
      * for indexed data frames
      */
    case m @ InMemoryRelationMatcher(output, storageLevel, child) if isIndexed(child) =>
      child match {
        case _ : IndexedOperatorExec => IndexedBlockRDD(output, getIfCached(child), child)
        case _ => m
      }

    /**
      * apply indexed join only on indexed data
      */
    case p @ Join(left, right, joinType, condition) if isIndexed(p) =>
      left match {
        case _ : IndexedOperator => IndexedJoin(left.asInstanceOf[IndexedOperator], right, joinType, condition)
        case _ => p
      }


    /**
      * apply indexed filtering only on filtered data
      */
    case p @ Filter(condition, child) if isIndexed(child) =>
      IndexedFilter(condition, child.asInstanceOf[IndexedOperator])

  }
}
