package it.bancaditalia.oss.vtl.impl.environment.dataset

import java.util.Spliterator.CONCURRENT
import java.util.Spliterator.DISTINCT
import java.util.Spliterator.IMMUTABLE
import java.util.Spliterator.NONNULL
import java.util.Spliterator.SIZED
import java.util.Spliterator.SUBSIZED
import java.util.Spliterators.AbstractSpliterator
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

import scala.collection.JavaConverters.asScalaSetConverter

import org.apache.spark.sql.Row

import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder
import it.bancaditalia.oss.vtl.model.data.DataPoint
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent
import java.util.Spliterator
import java.util.concurrent.atomic.AtomicBoolean

class DFSpliterator(sds: SparkDataSet) extends Spliterator[DataPoint] /*(sds.size, CONCURRENT + DISTINCT + IMMUTABLE + NONNULL + SIZED + SUBSIZED)*/ {

	val rowMapper = SparkUtils.rowMapper(sds.structure, sds.structure.asScala.map(c => c.getName -> c).toMap[String, DataStructureComponent[_, _, _]], sds.df.columns) _
	val done = new AtomicBoolean(false) 

	override def forEachRemaining(action: Consumer[_ >: DataPoint]) = {
		if (!done.getAndSet(true))
			sds.df.foreach(row => action.accept(rowMapper(row)))
	}

	override def trySplit(): Spliterator[DataPoint] = {
		null
	}

	override def tryAdvance(action: Consumer[_ >: DataPoint]): Boolean = {
		forEachRemaining(action);
		false
	}

	def characteristics() = DISTINCT + IMMUTABLE + NONNULL + SIZED + SUBSIZED

	def estimateSize() = sds.df.rdd.cache().count()
}