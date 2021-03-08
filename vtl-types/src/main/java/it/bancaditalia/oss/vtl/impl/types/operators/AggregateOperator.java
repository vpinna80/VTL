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

import static java.util.stream.Collector.Characteristics.UNORDERED;
import static java.util.stream.Collectors.averagingDouble;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.maxBy;
import static java.util.stream.Collectors.minBy;
import static java.util.stream.Collectors.summingDouble;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public enum AggregateOperator  
{
	COUNT("count", (dp, m) -> null, collectingAndThen(counting(), IntegerValue::new)),
	SUM("sum", collectingAndThen(summingDouble(v -> ((NumberValue<?, ?, ?>)v).get().doubleValue()), DoubleValue::new)), 
	AVG("avg", collectingAndThen(averagingDouble(v -> ((NumberValue<?, ?, ?>)v).get().doubleValue()), DoubleValue::new)),
	MEDIAN("median", collectingAndThen(mapping(NumberValue.class::cast, mapping(NumberValue::get, mapping(Number.class::cast, mapping(Number::doubleValue, 
			toList())))), l -> {
				List<Double> c = new ArrayList<>(l);
				Collections.sort(c);
				int s = c.size();
				return new DoubleValue(s % 2 == 0 ? c.get(s / 2) : (c.get(s /2) + c.get(s / 2 + 1)) / 2);
			})),
	MIN("min", collectingAndThen(minBy(ScalarValue::compareTo), v -> v.orElse(new DoubleValue(Double.NaN)))),
	MAX("max", collectingAndThen(maxBy(ScalarValue::compareTo), v -> v.orElse(new DoubleValue(Double.NaN)))),
	VAR_POP("stddev_pop", collectingAndThen(Collectors.mapping(v -> ((NumberValue<?, ?, ?>)v).get().doubleValue(), Collector.of( // See https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
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
	        acu -> acu[2] / acu[0], UNORDERED)), DoubleValue::new)),
	VAR_SAMP("stddev_samp", collectingAndThen(Collectors.mapping(v -> ((NumberValue<?, ?, ?>)v).get().doubleValue(), Collector.of( // See https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
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
	        acu -> acu[2] / (acu[0] + 1.0), UNORDERED)), DoubleValue::new)),
	STDDEV_POP("stddev.pop", collectingAndThen(VAR_POP.getReducer(), dv -> new DoubleValue(Math.sqrt((Double) dv.get())))),
	STDDEV_SAMP("stddev.var", collectingAndThen(VAR_SAMP.getReducer(), dv -> new DoubleValue(Math.sqrt((Double) dv.get()))));
	/*, 
	RANK = new AggregateOperator("RANK not implemented."),
	*/;

	private final Collector<ScalarValue<?, ?, ?>, ?, ScalarValue<?, ?, ?>> reducer;
	private final BiFunction<? super DataPoint, ? super DataStructureComponent<? extends Measure, ?, ?>, ScalarValue<?, ?, ?>> extractor;
	private final String name;

	private AggregateOperator(String name, Collector<ScalarValue<?, ?, ?>, ?, ScalarValue<?, ?, ?>> reducer)
	{
		this(name, DataPoint::get, reducer);
	}

	private AggregateOperator(String name,
			BiFunction<? super DataPoint, ? super DataStructureComponent<? extends Measure, ?, ?>, ScalarValue<?, ?, ?>> extractor,
			Collector<ScalarValue<?, ?, ?>, ?, ScalarValue<?, ?, ?>> reducer)
	{
		this.name = name;
		this.extractor = extractor;
		this.reducer = reducer;
	}

	public Collector<ScalarValue<?, ?, ?>, ?, ScalarValue<?, ?, ?>> getReducer()
	{
		return reducer;
	}
	
	public Collector<DataPoint, ?, ScalarValue<?, ?, ?>> getReducer(DataStructureComponent<? extends Measure, ?, ?> measure)
	{
		return Collectors.mapping(dp -> extractor.apply(dp, measure), reducer);
	}
	
	@Override
	public String toString()
	{
		return name;
	}
	
	public BiFunction<? super DataPoint, ? super DataStructureComponent<Measure, ?, ?>, ? extends ScalarValue<?, ?, ?>> getExtractor()
	{
		return extractor;
	}
}