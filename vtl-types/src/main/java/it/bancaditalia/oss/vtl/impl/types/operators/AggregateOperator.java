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

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.USE_BIG_DECIMAL;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NULLDS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.averagingBigDecimal;
import static it.bancaditalia.oss.vtl.util.SerCollectors.averagingDouble;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.counting;
import static it.bancaditalia.oss.vtl.util.SerCollectors.filtering;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.maxBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.minBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.summingBigDecimal;
import static it.bancaditalia.oss.vtl.util.SerCollectors.summingDouble;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static it.bancaditalia.oss.vtl.util.Utils.splittingConsumer;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.UNORDERED;

import java.math.BigDecimal;
import java.util.AbstractMap.SimpleEntry;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collector.Characteristics;

import it.bancaditalia.oss.vtl.config.VTLGeneralProperties;
import it.bancaditalia.oss.vtl.impl.types.data.BigDecimalValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageGroup;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.SerBiConsumer;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerCollectors;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.Utils;

public enum AggregateOperator  
{
	COUNT("count", (dp, m) -> null, collectingAndThen(counting(), IntegerValue::of)),
	SUM("sum", getSummingCollector()), 
	AVG("avg", getAveragingCollector()),
	MEDIAN("median", collectingAndThen(mapping(NumberValue.class::cast, toList()), l -> {
				List<NumberValue<?, ?, ?, ?>> sorted = Utils.getStream(l).sorted().collect(toList());
				int s = sorted.size();
				return s % 2 == 0 ? sorted.get(s / 2) : average(sorted.get(s >> 1), sorted.get(s / 2 + 1));
			})),
	MIN("min", collectingAndThen(minBy(ScalarValue::compareTo), opt -> opt.orElse(NullValue.instance(NULLDS)))),
	MAX("max", collectingAndThen(maxBy(ScalarValue::compareTo), opt -> opt.orElse(NullValue.instance(NULLDS)))),
	// See https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
	VAR_POP("stddev_pop", collectingAndThen(mapping(v -> ((NumberValue<?, ?, ?, ?>)v).get().doubleValue(), SerCollector.of( 
	        () -> new double[3],
	        (acu, d) -> {
	            acu[0]++;
	            double delta = d - acu[1];
	            acu[1] += delta / acu[0];
	            acu[2] += delta * (d - acu[1]);
	        },
	        (acuA, acuB) -> { 
	            double delta = acuB[1] - acuA[1];
	            double count = acuA[0] + acuB[0];
	            acuA[2] = acuA[2] + acuB[2] + delta * delta * acuA[0] * acuB[0] / count; // M2
	            acuA[1] += delta * acuB[0] / count; 
	            acuA[0] = count;
	            return acuA;
	        },
	        acu -> acu[2] / acu[0], EnumSet.of(UNORDERED))), DoubleValue::of)),
	// See https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
	VAR_SAMP("stddev_samp", collectingAndThen(mapping(v -> ((NumberValue<?, ?, ?, ?>)v).get().doubleValue(), SerCollector.of( 
	        () -> new double[3],
	        (acu, d) -> {
	            acu[0]++;
	            double delta = d - acu[1];
	            acu[1] += delta / acu[0];
	            acu[2] += delta * (d - acu[1]);
	        },
	        (acuA, acuB) -> { 
	            double delta = acuB[1] - acuA[1];
	            double count = acuA[0] + acuB[0];
	            acuA[2] = acuA[2] + acuB[2] + delta * delta * acuA[0] * acuB[0] / count; // M2
	            acuA[1] += delta * acuB[0] / count; 
	            acuA[0] = count;
	            return acuA;
	        },
	        acu -> acu[2] / (acu[0] + 1.0), EnumSet.of(UNORDERED))), DoubleValue::of)),
	STDDEV_POP("stddev.pop", collectingAndThen(VAR_POP.getReducer(), dv -> DoubleValue.of(Math.sqrt((Double) dv.get())))),
	STDDEV_SAMP("stddev.var", collectingAndThen(VAR_SAMP.getReducer(), dv -> DoubleValue.of(Math.sqrt((Double) dv.get()))));

	private final SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> reducer;
	private final BiFunction<? super DataPoint, ? super DataStructureComponent<? extends Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> extractor;
	private final String name;

	private static class CombinedAccumulator 
	{
		final Map<DataStructureComponent<? extends Measure, ?, ?>, Object> accs;
		final Map<Lineage, Long> lineageAcc;
		final Map<DataStructureComponent<? extends Measure, ?, ?>, AtomicBoolean> allIntegers;

		public CombinedAccumulator(Map<DataStructureComponent<? extends Measure, ?, ?>, SerCollector<DataPoint, ?, ScalarValue<?, ?, ?, ?>>> collectors)
		{
			accs = Utils.getStream(collectors).collect(SerCollectors.mappingValues(collector -> collector.supplier().get()));
			lineageAcc = new ConcurrentHashMap<>();
			allIntegers = Utils.getStream(collectors.keySet()).collect(SerCollectors.toMapWithValues(m -> new AtomicBoolean(INTEGERDS.isAssignableFrom(m.getDomain()))));
		}
	}

	private AggregateOperator(String name, SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> reducer)
	{
		this(name, (dp, c) -> dp.get(c), reducer);
	}

	private static SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> getSummingCollector()
	{
		if (Boolean.valueOf(VTLGeneralProperties.USE_BIG_DECIMAL.getValue()))
			return collectingAndThen(summingBigDecimal(v -> (BigDecimal) v.get()), BigDecimalValue::of);
		else
			return collectingAndThen(summingDouble(v -> ((Number) v.get()).doubleValue()), v -> DoubleValue.of(v));
	}

	private static SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> getAveragingCollector()
	{
		if (Boolean.valueOf(VTLGeneralProperties.USE_BIG_DECIMAL.getValue()))
			return collectingAndThen(averagingBigDecimal(v -> (BigDecimal) v.get()), BigDecimalValue::of);
		else
			return collectingAndThen(averagingDouble(v -> ((Number) v.get()).doubleValue()), v -> DoubleValue.of(v));
	}

	private static NumberValue<?, ?, ?, ?> average(NumberValue<?, ?, ?, ?> left, NumberValue<?, ?, ?, ?> right)
	{
		Number leftN = left.get(), rightN = right.get();
		if (Boolean.valueOf(USE_BIG_DECIMAL.getValue()))
		{
			leftN = leftN instanceof BigDecimal ? leftN : BigDecimal.valueOf(left.get().doubleValue());
			rightN = rightN instanceof BigDecimal ? rightN : BigDecimal.valueOf(right.get().doubleValue());
			return (NumberValue<?, ?, ?, ?>) BigDecimalValue.of(((BigDecimal) leftN).add((BigDecimal) rightN).divide(BigDecimal.valueOf(2)));
		}
		else
			return (NumberValue<?, ?, ?, ?>) DoubleValue.of((leftN.doubleValue() + rightN.doubleValue()) / 2);
	}

	private AggregateOperator(String name,
			BiFunction<? super DataPoint, ? super DataStructureComponent<? extends Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> extractor,
			SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> reducer)
	{
		this.name = name;
		this.extractor = extractor;
		this.reducer = reducer;
	}

	public SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> getReducer()
	{
		return reducer;
	}
	
	public SerCollector<DataPoint, ?, Entry<Lineage, Map<DataStructureComponent<? extends Measure, ?, ?>, ScalarValue<?, ?, ?, ?>>>> getReducer(Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures)
	{
		// Create a collector for each measure
		Map<DataStructureComponent<? extends Measure, ?, ?>, SerCollector<DataPoint, ?, ScalarValue<?, ?, ?, ?>>> collectors = measures.stream()
			.collect(SerCollectors.toMapWithValues(measure -> mapping(dp -> extractor.apply(dp, measure), filtering(v -> !(v instanceof NullValue), reducer))));
		
		// and-reduce the characteristics
		EnumSet<Characteristics> characteristics = EnumSet.of(CONCURRENT, UNORDERED);
		for (SerCollector<?, ?, ?> collector: collectors.values())
			characteristics.retainAll(collector.characteristics());

		// Combine all collectors into one
		return SerCollector.of(
				() -> new CombinedAccumulator(collectors), 
				(a, dp) -> {
					collectors.forEach((measure, collector) -> {
							@SuppressWarnings("unchecked")
							SerBiConsumer<Object, DataPoint> accumulator = (SerBiConsumer<Object, DataPoint>) collector.accumulator();
							accumulator.accept(a.accs.get(measure), dp);
							if (this != COUNT)
								a.allIntegers.get(measure).compareAndSet(true, INTEGERDS.isAssignableFrom(dp.get(measure).getDomain()));
						});
					a.lineageAcc.merge(dp.getLineage(), 1L, Long::sum);
				}, (a, b) -> { 
					Utils.getStream(b.accs)
						.forEach(splittingConsumer((measure, accumulator) -> {
							@SuppressWarnings("unchecked")
							SerBinaryOperator<Object> combiner = (SerBinaryOperator<Object>) collectors.get(measure).combiner();
							a.accs.merge(measure, accumulator, combiner);
						}));
					Utils.getStream(b.lineageAcc)
						.forEach(splittingConsumer((lineage, value) -> a.lineageAcc.merge(lineage, value, Long::sum)));
					Utils.getStream(b.allIntegers)
						.forEach(splittingConsumer((measure, allInt) -> a.allIntegers.merge(measure, allInt, (va, vb) -> { va.compareAndSet(true, vb.get()); return va; })));
					return a; 
				}, a -> new SimpleEntry<>(LineageGroup.of(a.lineageAcc), Utils.getStream(collectors).collect(toConcurrentMap(Entry::getKey, splitting((measure, collector) -> {
					@SuppressWarnings("unchecked")
					SerFunction<Object, ScalarValue<?, ?, ?, ?>> finisher = (SerFunction<Object, ScalarValue<?, ?, ?, ?>>) collector.finisher(); 
					ScalarValue<?, ?, ?, ?> result = finisher.apply(a.accs.get(measure));
					if (a.allIntegers.get(measure).get() && result instanceof DoubleValue)
						return IntegerValue.of(((DoubleValue<?>) result).get().longValue());
					else
						return result;
				})))), characteristics);
	}

	@Override
	public String toString()
	{
		return name;
	}
	
	public BiFunction<? super DataPoint, ? super DataStructureComponent<Measure, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> getExtractor()
	{
		return extractor;
	}
}