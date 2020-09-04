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