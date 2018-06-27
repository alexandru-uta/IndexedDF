
package indexeddataframe

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import indexeddataframe.execution._
import indexeddataframe.logical._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys

/**
  * strategies for the operators applied on indexed dataframes
  */
object IndexedOperators extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateIndex(colNo, child) => CreateIndexExec(colNo, planLater(child)) :: Nil
    case AppendRows(left, right) => AppendRowsExec(planLater(left), planLater(right)) :: Nil
    /**
      * this is a strategy for eliminating the [InMemoryRelation] that spark generates when .cache() is called
      * on an ordinary dataframe; in that case, the representation of the data frame is changed to a CachedBatch;
      * we cannot have that on the indexed data frames as we would lose the indexing capabilities; therefore, we just
      * insert a dummy strategy that returns an operator which works on "indexed RDDs"
      */
    case IndexedBlockRDD(output, rdd, child : IndexedOperatorExec) =>
      IndexedBlockRDDScanExec(output, rdd, child.asInstanceOf[IndexedOperatorExec]) :: Nil

    case GetRows(key, child) => GetRowsExec(key, planLater(child)) :: Nil
    /**
      * dummy filter object for the moment; in the future, we will implement filtering functionality on the indexed data
      */
    case IndexedFilter(condition, child) => IndexedFilterExec(condition, planLater(child)) :: Nil
    case IndexedJoin(left, right, joinType, condition) =>
      Join(left, right, joinType, condition) match {
        case ExtractEquiJoinKeys(_, leftKeys, rightKeys, _, lChild, rChild) => {
          // compute the index of the left side keys == column number
          var leftColNo = 0
          var i = 0
          lChild.output.foreach( col => {
            if (col == leftKeys(0)) leftColNo = i
            i += 1
          })

          // compute the index of the right side keys == column number
          var rightColNo = 0
          i = 0
          rChild.output.foreach( col => {
            if (col == rightKeys(0)) rightColNo = i
            i += 1
          })

          //println("leftcol = %d, rightcol = %d".format(leftColNo, rightColNo))
          IndexedShuffledEquiJoinExec(planLater(left), planLater(right), leftColNo, rightColNo, leftKeys, rightKeys) :: Nil
        }
        case _ => Nil
      }
    case _ => Nil
  }
}
