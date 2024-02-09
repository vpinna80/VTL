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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NULLDS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.averagingDouble;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.counting;
import static it.bancaditalia.oss.vtl.util.SerCollectors.filtering;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.maxBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.minBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.reducing;
import static it.bancaditalia.oss.vtl.util.SerCollectors.summingDouble;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static java.util.stream.Collector.Characteristics.UNORDERED;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerCollector;

public enum AnalyticOperator  
{
	COUNT("count", (dp, m) -> null, collectingAndThen(counting(), IntegerValue::of)),
	SUM("sum", collectingAndThen(filtering(v -> !(v instanceof NullValue), summingDouble(v -> ((NumberValue<?, ?, ?, ?>)v).get().doubleValue())), DoubleValue::of)), 
	AVG("avg", collectingAndThen(filtering(v -> !(v instanceof NullValue), averagingDouble(v -> ((NumberValue<?, ?, ?, ?>)v).get().doubleValue())), DoubleValue::of)),
	MEDIAN("median", collectingAndThen(mapping(NumberValue.class::cast, mapping(NumberValue::get, mapping(Number.class::cast, mapping(Number::doubleValue, 
			toList())))), l -> {
				List<Double> c = new ArrayList<>(l);
				Collections.sort(c);
				int s = c.size();
				return NumberValueImpl.createNumberValue(s % 2 == 0 ? c.get(s / 2) : (c.get(s /2) + c.get(s / 2 + 1)) / 2);
			})),
	MIN("min", collectingAndThen(filtering(v -> !(v instanceof NullValue), minBy(ScalarValue::compareTo)), v -> v.orElse(NullValue.instance(NULLDS)))),
	MAX("max", collectingAndThen(filtering(v -> !(v instanceof NullValue), maxBy(ScalarValue::compareTo)), v -> v.orElse(NullValue.instance(NULLDS)))),
	// See https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
	VAR_POP("var_pop", collectingAndThen(mapping(v -> ((NumberValue<?, ?, ?, ?>)v).get().doubleValue(), SerCollector.of(
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
	        acu -> acu[2] / acu[0], EnumSet.of(UNORDERED))), NumberValueImpl::createNumberValue)),
	// See https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
	VAR_SAMP("var_samp", collectingAndThen(mapping(v -> ((NumberValue<?, ?, ?, ?>) v).get().doubleValue(), SerCollector.of( 
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
	        acu -> acu[2] / (acu[0] + 1.0), EnumSet.of(UNORDERED))), NumberValueImpl::createNumberValue)),
	STDDEV_POP("stddev_pop", collectingAndThen(VAR_POP.getReducer(), dv -> NumberValueImpl.createNumberValue(Math.sqrt((Double) dv.get())))),
	STDDEV_SAMP("stddev_var", collectingAndThen(VAR_SAMP.getReducer(), dv -> NumberValueImpl.createNumberValue(Math.sqrt((Double) dv.get())))),
	FIRST_VALUE("first_value", collectingAndThen(reducing((a, b) -> a), o -> o.orElse(NullValue.instance(NULLDS)))),
	LAST_VALUE("last_value", collectingAndThen(reducing((a, b) -> b), o -> o.orElse(NullValue.instance(NULLDS))));

	private final SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> reducer;
	private final SerBiFunction<? super DataPoint, ? super DataStructureComponent<? extends Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> extractor;
	private final String name;

	private AnalyticOperator(String name, SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> reducer)
	{
		this(name, DataPoint::get, reducer);
	}

	private AnalyticOperator(String name,
			SerBiFunction<? super DataPoint, ? super DataStructureComponent<? extends Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> extractor,
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
	
	public SerCollector<DataPoint, ?, ScalarValue<?, ?, ?, ?>> getReducer(DataStructureComponent<? extends Measure, ?, ?> measure)
	{
		return mapping(dp -> extractor.apply(dp, measure), filtering(v -> !(v instanceof NullValue), reducer));
	}
	
	@Override
	public String toString()
	{
		return name;
	}
	
	public SerBiFunction<? super DataPoint, ? super DataStructureComponent<Measure, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> getExtractor()
	{
		return extractor;
	}
}