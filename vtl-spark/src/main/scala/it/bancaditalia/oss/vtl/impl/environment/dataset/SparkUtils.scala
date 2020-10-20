/**
 * Copyright © 2020 Banca D'Italia
 *
 * Licensed under the EUPL, Version 1.2 (the "License");
 * You may not use this work except in compliance with the
 * License.
 * You may obtain a copy of the License at:
 *
 * https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the License is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 *
 * See the License for the specific language governing
 * permissions and limitations under the License.
 */
package it.bancaditalia.oss.vtl.impl.environment.dataset

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType

import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder
import it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN
import it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGER
import it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBER
import it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent
import it.bancaditalia.oss.vtl.model.data.ScalarValue
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset
import it.bancaditalia.oss.vtl.exceptions.VTLException

object SparkUtils {
  
  def toVTLValue(value: Any, domain: ValueDomainSubset[_]): ScalarValue[_, _, _] = {
    ???
  }

  def toScalaValue[T <: Comparable[_] with Serializable](value: ScalarValue[T, _, _]): T = value.get

  def sparkType(domain: ValueDomainSubset[_]) : DataType = {
    if (BOOLEAN.isAssignableFrom(domain))
      BooleanType
    else if (STRING.isAssignableFrom(domain))
      StringType
    else if (INTEGER.isAssignableFrom(domain))
      LongType
    else if (NUMBER.isAssignableFrom(domain))
      DoubleType
    else
      throw new VTLException("VTL domain " + domain + " is not yet mapped in spark.");
  }

  def rowMapper(structure: DataSetMetadata, namemap: Map[String, DataStructureComponent[_, _, _]], names: Array[String])(row: Row) = {
    new DataPointBuilder(row.toSeq.zipWithIndex.map({
            case (v, i) => namemap(names(i)) -> SparkUtils.toVTLValue(v, namemap(names(i)).getDomain)
          }).toMap[DataStructureComponent[_, _, _], ScalarValue[_, _, _]].asJava)
          .build(structure)
  }
}