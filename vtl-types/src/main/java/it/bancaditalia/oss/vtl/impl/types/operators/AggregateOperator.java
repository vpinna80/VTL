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
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NULLDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.counting;
import static it.bancaditalia.oss.vtl.util.SerCollectors.filtering;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.maxBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.minBy;
import static it.bancaditalia.oss.vtl.util.SerPredicate.not;
import static java.util.stream.Collector.Characteristics.CONCURRENT;
import static java.util.stream.Collector.Characteristics.UNORDERED;

import java.math.BigDecimal;
import java.util.EnumSet;

import it.bancaditalia.oss.vtl.impl.types.data.BigDecimalValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerCollectors;
import it.bancaditalia.oss.vtl.util.SerDoubleSumAvgCount;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerSupplier;

public enum AggregateOperator  
{
	COUNT(() -> collectingAndThen(counting(), IntegerValue::of)),
	SUM(() -> getSummingCollector()), 
	AVG(() -> getAveragingCollector()),
	MEDIAN(() -> collectingAndThen(filtering(not(NullValue.class::isInstance), new MedianCollector(getSVClass())), opt -> opt.orElse(NullValue.instance(NULLDS)))),
	MIN(() -> collectingAndThen(minBy(getSVClass(), ScalarValue::compareTo), opt -> opt.orElse(NullValue.instance(NULLDS)))),
	MAX(() -> collectingAndThen(maxBy(getSVClass(), ScalarValue::compareTo), opt -> opt.orElse(NullValue.instance(NULLDS)))),
	VAR_POP(() -> collectingAndThen(SerCollectors.mapping(v -> (Number) v.get(), varianceCollector(acu -> acu[2] / (acu[0] + 1))), NumberValueImpl::createNumberValue)),
	VAR_SAMP(() -> collectingAndThen(mapping(v -> (Number) v.get(), varianceCollector(acu -> acu[2] / acu[0])), NumberValueImpl::createNumberValue)),
	STDDEV_POP(() -> collectingAndThen(VAR_POP.getReducer(), dv -> createNumberValue(Math.sqrt((Double) dv.get())))),
	STDDEV_SAMP(() -> collectingAndThen(VAR_SAMP.getReducer(), dv -> createNumberValue(Math.sqrt((Double) dv.get()))));

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static final Class<ScalarValue<?, ?, ?, ?>> CLASS = (Class<ScalarValue<?, ?, ?, ?>>) (Class<? extends ScalarValue>) ScalarValue.class;

	private static Class<ScalarValue<?, ?, ?, ?>> getSVClass()
	{
		return CLASS;
	}

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
	
	private final SerSupplier<SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>>> reducerSupplier;
	
	private AggregateOperator(SerSupplier<SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>>> reducerSupplier)
	{
		this.reducerSupplier = reducerSupplier;
	}
	
	static SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> getSummingCollector()
	{
		return SerCollector.of(SerDoubleSumAvgCount::new, SerDoubleSumAvgCount::accumulate, SerDoubleSumAvgCount::combine, AggregateOperator::sum, EnumSet.of(UNORDERED, CONCURRENT));
	}

	static SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> getAveragingCollector()
	{
		return SerCollector.of(SerDoubleSumAvgCount::new, SerDoubleSumAvgCount::accumulate, SerDoubleSumAvgCount::combine, AggregateOperator::avg, EnumSet.of(UNORDERED, CONCURRENT));
	}

	public SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> getReducer()
	{
		return reducerSupplier.get();
	}

	@Override
	public String toString()
	{
		return super.toString().toLowerCase();
	}
    
	private static ScalarValue<?, ?, ?, ?> sum(SerDoubleSumAvgCount count)
	{
		if (count.getCount() == 0)
			return IntegerValue.of((Long) null);
		else if (count.getCountDouble() == 0)
			return IntegerValue.of(count.getLongSum());
		else if (isUseBigDecimal())
			return BigDecimalValue.of(count.getBigDecimalSum().add(new BigDecimal(count.getLongSum())));
		else
			return DoubleValue.of(count.getDoubleSum() + count.getLongSum(), NUMBERDS);
	}
	
	private static ScalarValue<?, ?, ?, ?> avg(SerDoubleSumAvgCount count)
	{
		int c = count.getCount();
		
		if (c == 0)
			return createNumberValue((Number) null);
		else if (isUseBigDecimal())
			return BigDecimalValue.of(count.getBigDecimalSum().add(new BigDecimal(count.getLongSum()).divide(new BigDecimal(c))));
		else
			return DoubleValue.of((count.getDoubleSum() + count.getLongSum()) / c, NUMBERDS);
	}
}