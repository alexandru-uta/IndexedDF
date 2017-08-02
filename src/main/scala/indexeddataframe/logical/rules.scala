package indexeddataframe.logical

import indexeddataframe.execution.IndexedOperatorExec
import indexeddataframe.Utils
import org.apache.spark.sql.InMemoryRelationMatcher
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import indexeddataframe.logical.IndexedLocalRelation

object IndexLocalRelation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case CreateIndex(colNo, LocalRelation(output, data)) =>
      IndexedLocalRelation(output, data)
  }
}

object ConvertToIndexedOperators extends Rule[LogicalPlan] {
  def isIndexed(plan: SparkPlan): Boolean = {
    plan.find {
      case _: IndexedOperatorExec => true
      case _ => false
    }.nonEmpty
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case InMemoryRelationMatcher(output, storageLevel, child) if isIndexed(child) =>
      IndexedBlockRDD(
        output,
        Utils.ensureCached(
          child.asInstanceOf[IndexedOperatorExec].executeIndexed()))
  }
}
