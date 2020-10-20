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

import java.util.Map
import java.util.Spliterator.DISTINCT
import java.util.Spliterator.IMMUTABLE
import java.util.Spliterator.NONNULL
import java.util.Spliterator.SIZED
import java.util.Spliterator.SUBSIZED
import java.util.stream.Stream
import java.util.stream.StreamSupport

import scala.collection.JavaConverters.asScalaSetConverter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier
import it.bancaditalia.oss.vtl.model.data.DataPoint
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent
import it.bancaditalia.oss.vtl.model.data.ScalarValue
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset

class SparkDataSet(spark: SparkSession, val df: DataFrame, val structure: DataSetMetadata) extends AbstractDataSet(structure) {
	override val size = df.count
	type KeyMap = Map[DataStructureComponent[Identifier, _, _], ScalarValue[_, _, _]]

	private val sparkStructure = StructType(structure.asScala
		.map(c => StructField(c.getName, SparkUtils.sparkType(c.getDomain.asInstanceOf[ValueDomainSubset[_]]), true)).toList)

	def streamDataPoints: Stream[DataPoint] = {
		StreamSupport.stream(() => new DFSpliterator(this), DISTINCT + IMMUTABLE + NONNULL + SIZED + SUBSIZED, false)
	}
}