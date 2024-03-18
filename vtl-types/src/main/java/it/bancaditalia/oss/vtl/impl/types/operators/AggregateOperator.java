/*
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
package it.bancaditalia.oss.vtl.impl.types.operators;

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.isUseBigDecimal;
import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NULLDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.averagingDouble;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.counting;
import static it.bancaditalia.oss.vtl.util.SerCollectors.filtering;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mappingValues;
import static it.bancaditalia.oss.vtl.util.SerCollectors.maxBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.minBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.summingBigDecimal;
import static it.bancaditalia.oss.vtl.util.SerCollectors.summingDouble;
import static it.bancaditalia.oss.vtl.util.SerCollectors.summingLong;
import static it.bancaditalia.oss.vtl.util.SerCollectors.teeing;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static it.bancaditalia.oss.vtl.util.Utils.splittingConsumer;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.UNORDERED;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collector;
import java.util.stream.Collector.Characteristics;

import it.bancaditalia.oss.vtl.impl.types.data.BigDecimalValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.SerBiConsumer;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.Utils;

public enum AggregateOperator  
{
	COUNT("count", null),
	SUM("sum", getSummingCollector()), 
	AVG("avg", getAveragingCollector()),
	MEDIAN("median", collectingAndThen(toSet(), s -> {
				ScalarValue<?, ?, ?, ?>[] array = s.toArray(new ScalarValue<?, ?, ?, ?>[s.size()]);
				if (Utils.SEQUENTIAL) 
					Arrays.sort(array);
				else
					Arrays.parallelSort(array);
				return array[array.length / 2];
			})),
	MIN("min", collectingAndThen(minBy(ScalarValue::compareTo), opt -> opt.orElse(NullValue.instance(NULLDS)))),
	MAX("max", collectingAndThen(maxBy(ScalarValue::compareTo), opt -> opt.orElse(NullValue.instance(NULLDS)))),
	VAR_POP("stddev_pop", collectingAndThen(mapping(v -> (Number) v.get(), varianceCollector(acu -> acu[2] / (acu[0] + 1))), NumberValueImpl::createNumberValue)),
	VAR_SAMP("stddev_samp", collectingAndThen(mapping(v -> (Number) v.get(), varianceCollector(acu -> acu[2] / acu[0])), NumberValueImpl::createNumberValue)),
	STDDEV_POP("stddev.pop", collectingAndThen(VAR_POP.getReducer(), dv -> createNumberValue(Math.sqrt((Double) dv.get())))),
	STDDEV_SAMP("stddev.var", collectingAndThen(VAR_SAMP.getReducer(), dv -> createNumberValue(Math.sqrt((Double) dv.get()))));

	private static final DataStructureComponent<Measure, ?, ?> COUNT_MEASURE = DataStructureComponentImpl.of(Measure.class, INTEGERDS);
	private static final SerFunction<? super DataStructureComponent<?, ?, ?>, ? extends AtomicBoolean> FLAGMAP = (SerFunction<? super DataStructureComponent<?, ?, ?>, ? extends AtomicBoolean>) key -> new AtomicBoolean(false);

	// See https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
	private static SerCollector<Number, ?, Double> varianceCollector(SerFunction<double[], Double> finalizer)
	{
		return SerCollector.of(() -> new double[3], (acu, v) -> {
	        	double d = v.doubleValue();
	        	double delta = d - acu[1];
	            acu[0]++;
	            acu[1] += delta / acu[0];
	            acu[2] += delta * (d - acu[1]);
	        }, (acuA, acuB) -> { 
		            double delta = acuB[1] - acuA[1];
		            double count = acuA[0] + acuB[0];
		            acuA[2] = acuA[2] + acuB[2] + delta * delta * acuA[0] * acuB[0] / count; // M2
		            acuA[1] += delta * acuB[0] / count; 
		            acuA[0] = count;
		            return acuA;
	        }, finalizer, EnumSet.of(UNORDERED));
	}
	
	private final SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> reducer;
	private final String name;
	
	private AggregateOperator(String name, SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> reducer)
	{
		this.name = name;
		this.reducer = reducer;
	}
	
	private static SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> getSummingCollector()
	{
		if (isUseBigDecimal())
			return collectingAndThen(summingBigDecimal(v -> (BigDecimal) v.get()), BigDecimalValue::of);
		else
			return teeing(filtering(v -> v instanceof DoubleValue, counting()),
				filtering(v -> !(v instanceof NullValue), teeing(
						summingDouble(v -> ((Number) v.get()).doubleValue()), 
						summingLong(v -> ((Number) v.get()).longValue()), 
						SimpleEntry::new)
				), (c, e) -> (c > 0 ? (ScalarValue<?, ?, ?, ?>) DoubleValue.of(e.getKey()) : (ScalarValue<?, ?, ?, ?>) IntegerValue.of(e.getValue())));
	}

	private static SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> getAveragingCollector()
	{
		return collectingAndThen(filtering(v -> v instanceof NumberValue, mapping(ScalarValue::get, averagingDouble(v -> ((Number) v).doubleValue()))), DoubleValue::of);
	}

	public SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> getReducer()
	{
		return reducer;
	}
	
	private static class CombinedAccumulator implements Serializable
	{
		private static final long serialVersionUID = 1L;
		
		final Map<DataStructureComponent<? extends Measure, ?, ?>, Serializable> accs;
		final Map<Lineage, Long> lineageAcc;
		final transient Map<DataStructureComponent<?, ?, ?>, AtomicBoolean> allIntegers;

		public CombinedAccumulator(Map<DataStructureComponent<? extends Measure, ?, ?>, SerCollector<DataPoint, ?, ScalarValue<?, ?, ?, ?>>> collectors)
		{
			accs = Utils.getStream(collectors).collect(mappingValues(collector -> (Serializable) collector.supplier().get()));
			lineageAcc = new ConcurrentHashMap<>();
			allIntegers = new ConcurrentHashMap<>();
		}
	}
	
	/**
	 * Create a {@link Collector} that reduces datapoints by combining the same measures in each datapoint according to this {@link AggregateOperator}
	 *   
	 * @param measures
	 * @return
	 */
	public SerCollector<DataPoint, ?, DataPoint> getReducer(Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures)
	{
		// Special collector for COUNT that collects all measures into one
		if (this == COUNT)
		{
			DataStructureComponent<Measure, ?, ?> measure = COUNT_MEASURE;
			DataSetMetadata structure = new DataStructureBuilder(COUNT_MEASURE).build();
			return collectingAndThen(counting(), c -> {
				return new DataPointBuilder()
					.add(measure, IntegerValue.of((long) c))
					.build(LineageExternal.of("count"), structure);
			});
		};

		// Create a collector for each measure
		Map<DataStructureComponent<? extends Measure, ?, ?>, SerCollector<DataPoint, ?, ScalarValue<?, ?, ?, ?>>> collectors = measures.stream()
			.collect(toMapWithValues(measure -> mapping(dp -> dp.get(measure), filtering(v -> !(v instanceof NullValue), reducer))));
		
		// and-reduce the characteristics
		EnumSet<Characteristics> characteristics = EnumSet.of(CONCURRENT, UNORDERED);
		for (SerCollector<?, ?, ?> collector: collectors.values())
			characteristics.retainAll(collector.characteristics());
		
		boolean isChanging = EnumSet.of(AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP).contains(this);
		Set<? extends DataStructureComponent<? extends Measure, ?, ?>> outputMeasures;
		if (isChanging)
			outputMeasures = measures.stream()
				.map(m -> INTEGERDS.isAssignableFrom(m.getVariable().getDomain()) ? DataStructureComponentImpl.of(m.getVariable().getName(), Measure.class, NUMBERDS) : m)
				.collect(toSet());
		else
			outputMeasures = measures;
		
		
		// Combine all collectors into one
		SerCollector<DataPoint, CombinedAccumulator, DataPoint> combined = SerCollector.of(
				() -> new CombinedAccumulator(collectors), 
				(a, dp) -> {
					collectors.forEach((SerBiConsumer<DataStructureComponent<?, ?, ?>, SerCollector<?, ?, ?>>) (measure, collector) -> {
							@SuppressWarnings("unchecked")
							SerBiConsumer<Object, DataPoint> accumulator = (SerBiConsumer<Object, DataPoint>) collector.accumulator();
							accumulator.accept(a.accs.get(measure), dp);
							if (this != COUNT)
								a.allIntegers.computeIfAbsent(measure, FLAGMAP).compareAndSet(true, INTEGERDS.isAssignableFrom(dp.get(measure).getDomain()));
						});
					a.lineageAcc.merge(dp.getLineage(), 1L, Long::sum);
				}, (a, b) -> { 
					Utils.getStream(b.accs)
						.forEach(splittingConsumer((measure, accumulator) -> {
							@SuppressWarnings("unchecked")
							SerBinaryOperator<Serializable> combiner = (SerBinaryOperator<Serializable>) collectors.get(measure).combiner();
							a.accs.merge(measure, accumulator, combiner);
						}));
					Utils.getStream(b.lineageAcc)
						.forEach(splittingConsumer((lineage, value) -> a.lineageAcc.merge(lineage, value, Long::sum)));
					Utils.getStream(b.allIntegers)
						.forEach(splittingConsumer((measure, allInt) -> a.allIntegers.merge(measure, allInt, (va, vb) -> { va.compareAndSet(true, vb.get()); return va; })));
					return a; 
				}, a -> new DataPointBuilder().addAll(Utils.getStream(collectors).collect(
						toConcurrentMap(e -> {
							DataStructureComponent<? extends Measure, ?, ?> m = e.getKey();
							if (this == COUNT)
								m = COUNT_MEASURE;
							else if (isChanging)
								m = INTEGERDS.isAssignableFrom(m.getVariable().getDomain()) ? DataStructureComponentImpl.of(m.getVariable().getName(), Measure.class, NUMBERDS) : m;
							return m;
						}, splitting((measure, collector) -> {
							@SuppressWarnings("unchecked")
							SerFunction<Object, ScalarValue<?, ?, ?, ?>> finisher = (SerFunction<Object, ScalarValue<?, ?, ?, ?>>) collector.finisher(); 
							ScalarValue<?, ?, ?, ?> result = finisher.apply(a.accs.get(measure));
							if (a.allIntegers.computeIfAbsent(measure, FLAGMAP).get() && result instanceof DoubleValue)
								return IntegerValue.of(((DoubleValue<?>) result).get().longValue());
							else
								return result;
						}))
				)).build(LineageNode.of(this.toString(), a.lineageAcc.keySet().toArray(new Lineage[a.lineageAcc.size()])), new DataStructureBuilder(outputMeasures).build()), characteristics);
		
		return combined;
	}

	@Override
	public String toString()
	{
		return name;
	}
}