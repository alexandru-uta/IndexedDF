/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package indexeddataframe

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import indexeddataframe.execution._
import indexeddataframe.logical._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys

object IndexedOperators extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateIndex(colNo, child) => CreateIndexExec(colNo, planLater(child)) :: Nil
    case AppendRows(rows, child) => AppendRowsExec(rows, planLater(child)) :: Nil
    case IndexedBlockRDD(output, rdd) => IndexedBlockRDDScanExec(output, rdd) :: Nil
    case GetRows(key, child) => GetRowsExec(key, planLater(child)) :: Nil
    case IndexedJoin(left, right, joinType, condition) =>
      Join(left, right, joinType, condition) match {
        case ExtractEquiJoinKeys(jointype, leftKeys, rightKeys, condition, lChild, rChild) => {

          println(condition)
          println(jointype)

          println(leftKeys.map(_.dataType))
          println(rightKeys.map(_.dataType))

          leftKeys.foreach( k => println(k) )
          rightKeys.foreach( k => println(k) )

          println(lChild)
          println(rChild)

          null
        }
        case _ => Nil
      }
    case _ => Nil
  }
}
