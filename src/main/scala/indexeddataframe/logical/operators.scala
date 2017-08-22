package indexeddataframe.logical

import indexeddataframe.IRDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.SparkPlan

case class CreateIndex(val colNo: Int, child: LogicalPlan) extends UnaryNode with IndexedOperator {
  override def output: Seq[Attribute] = child.output
}

case class AppendRows(val rows: Seq[InternalRow], child: LogicalPlan) extends UnaryNode with IndexedOperator {
  override def output: Seq[Attribute] = child.output
}

case class GetRows(val key: Long, child: LogicalPlan) extends UnaryNode with IndexedOperator {
  override def output: Seq[Attribute] = child.output
}

trait IndexedOperator extends LogicalPlan {
  /**
    * Every indexed operator relies on its input having a specific set of columns, so we override
    * references to include all inputs to prevent Catalyst from dropping any input columns.
    */
  override def references: AttributeSet = inputSet


  def isIndexed: Boolean = children.exists(_.find {
    case p: IndexedOperator => p.isIndexed
    case _ => false
  }.nonEmpty)

}

case class IndexedLocalRelation(output: Seq[Attribute], data: Seq[InternalRow])
  extends LeafNode with MultiInstanceRelation with IndexedOperator {

  // A local relation must have resolved output.
  require(output.forall(_.resolved), "Unresolved attributes found when constructing LocalRelation.")

  /**
    * Returns an identical copy of this relation with new exprIds for all attributes.  Different
    * attributes are required when a relation is going to be included multiple times in the same
    * query.
    */
  override final def newInstance(): this.type = {
    IndexedLocalRelation(output.map(_.newInstance()), data).asInstanceOf[this.type]
  }

  override protected def stringArgs = Iterator(output)

  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case IndexedLocalRelation(otherOutput, otherData) =>
      (otherOutput.map(_.dataType) == output.map(_.dataType) && otherData == data)
    case _ => false
  }
}

case class IndexedBlockRDD(output: Seq[Attribute], rdd: IRDD, child: SparkPlan)
  extends IndexedOperator with MultiInstanceRelation {

  override def children: Seq[LogicalPlan] = Nil

  override def newInstance(): IndexedBlockRDD.this.type =
    IndexedBlockRDD(output.map(_.newInstance()), rdd, child).asInstanceOf[this.type]

  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case IndexedBlockRDD(_, otherRDD, child) => rdd.id == otherRDD.id
    case _ => false
  }

  override def producedAttributes: AttributeSet = outputSet
}

case class IndexedJoin(left: LogicalPlan,
                          right: LogicalPlan,
                          joinType: JoinType,
                          condition: Option[Expression])
  extends BinaryNode with IndexedOperator {

  override def output: Seq[Attribute] = left.output ++ right.output
}

case class IndexedFilter(condition: Expression, child:IndexedOperator) extends UnaryNode with IndexedOperator {
  override def output: Seq[Attribute] = child.output
}