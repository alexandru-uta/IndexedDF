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

object ConvertToIndexedOperators extends Rule[LogicalPlan] {

  private val cachedPlan: TrieMap[SparkPlan, IRDD] = new TrieMap[SparkPlan, IRDD]

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

  def isIndexed(plan: LogicalPlan): Boolean = {
    plan.find {
      case _: IndexedOperator => true
      case _ => false
    }.nonEmpty
  }

  def isIndexed(plan: SparkPlan): Boolean = {
    plan.find {
      case _: IndexedOperatorExec => true
      case _ => false
    }.nonEmpty
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {

    case InMemoryRelationMatcher(output, storageLevel, child) if isIndexed(child) =>
      IndexedBlockRDD(output, getIfCached(child))

    case p @ Join(left, right, joinType, condition) if isIndexed(p) =>
      IndexedJoin(left.asInstanceOf[IndexedOperator], right, joinType, condition)

    case p @ Filter(condition, child) if isIndexed(child) =>
      IndexedFilter(condition, child.asInstanceOf[IndexedOperator])

  }
}
