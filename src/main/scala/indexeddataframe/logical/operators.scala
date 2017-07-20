package indexeddataframe.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

case class CreateIndex(val colNo: Int, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  //println(child)
  //println(colNo)
  //println(child.output)
}
