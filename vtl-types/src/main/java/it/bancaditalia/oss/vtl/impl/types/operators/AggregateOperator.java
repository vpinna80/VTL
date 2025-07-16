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

import static it.bancaditalia.oss.vtl.config.ConfigurationManager.isUseBigDecimal;
import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NULLDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.counting;
import static it.bancaditalia.oss.vtl.util.SerCollectors.filtering;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.maxBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.minBy;
import static it.bancaditalia.oss.vtl.util.SerPredicate.not;
import static java.lang.Math.sqrt;
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
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerDoubleSumAvgCount;
import it.bancaditalia.oss.vtl.util.SerFunction;

public enum AggregateOperator  
{
	COUNT(domain -> collectingAndThen(counting(), IntegerValue::of)),
	SUM(domain -> getSummingCollector(domain)),
	AVG(domain -> getAveragingCollector(domain)),
	MEDIAN(domain -> collectingAndThen(filtering(not(NullValue.class::isInstance), new MedianCollector(getSVClass())), opt -> opt.orElse(NullValue.instance(NULLDS)))),
	MIN(domain -> collectingAndThen(minBy(getSVClass(), ScalarValue::compareTo), opt -> opt.orElse(NullValue.instance(NULLDS)))),
	MAX(domain -> collectingAndThen(maxBy(getSVClass(), ScalarValue::compareTo), opt -> opt.orElse(NullValue.instance(NULLDS)))),
	VAR_POP(domain -> collectingAndThen(mapping(v -> (Number) v.get(), varianceCollector(acu -> acu[2] / acu[0])), NumberValueImpl::createNumberValue)),
	VAR_SAMP(domain -> collectingAndThen(mapping(v -> (Number) v.get(), varianceCollector(acc -> acc[2] / (acc[0] - 1))), NumberValueImpl::createNumberValue)),
	STDDEV_POP(domain -> collectingAndThen(mapping(v -> (Number) v.get(), varianceCollector(acu -> sqrt(acu[2] / acu[0]))), NumberValueImpl::createNumberValue)),
	STDDEV_SAMP(domain -> collectingAndThen(mapping(v -> (Number) v.get(), varianceCollector(acc -> sqrt(acc[2] / (acc[0] - 1)))), NumberValueImpl::createNumberValue));

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static final Class<ScalarValue<?, ?, ?, ?>> CLASS = (Class<ScalarValue<?, ?, ?, ?>>) (Class<? extends ScalarValue>) ScalarValue.class;

	private static Class<ScalarValue<?, ?, ?, ?>> getSVClass()
	{
		return CLASS;
	}

	// See https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
	private static SerCollector<Number, ?, Double> varianceCollector(SerFunction<double[], Double> finalizer)
	{
		// [count, 
		return SerCollector.of(() -> new double[3], (acc, v) -> {
	        	double d = v.doubleValue();
	        	double delta = d - acc[1];
	            acc[0]++;
	            acc[1] += delta / acc[0];
	            acc[2] += delta * (d - acc[1]);
	        }, (acuA, acuB) -> { 
		            double delta = acuB[1] - acuA[1];
		            double count = acuA[0] + acuB[0];
		            acuA[2] = acuA[2] + acuB[2] + delta * delta * acuA[0] * acuB[0] / count; // M2
		            acuA[1] += delta * acuB[0] / count; 
		            acuA[0] = count;
		            return acuA;
	        }, finalizer, EnumSet.of(UNORDERED));
	}
	
	private final SerFunction<ValueDomainSubset<?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>>> reducerSupplier;
	
	private AggregateOperator(SerFunction<ValueDomainSubset<?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>>> reducerSupplier)
	{
		this.reducerSupplier = reducerSupplier;
	}
	
	static SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> getSummingCollector(ValueDomainSubset<? ,?> targetDomain)
	{
		return SerCollector.of(() -> new SerDoubleSumAvgCount(INTEGERDS.isAssignableFrom(targetDomain)), SerDoubleSumAvgCount::accumulate, SerDoubleSumAvgCount::combine, AggregateOperator::sum, EnumSet.of(UNORDERED, CONCURRENT));
	}

	static SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> getAveragingCollector(ValueDomainSubset<? ,?> targetDomain)
	{
		return SerCollector.of(() -> new SerDoubleSumAvgCount(INTEGERDS.isAssignableFrom(targetDomain)), SerDoubleSumAvgCount::accumulate, SerDoubleSumAvgCount::combine, AggregateOperator::avg, EnumSet.of(UNORDERED, CONCURRENT));
	}

	public SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> getReducer(ValueDomainSubset<?, ?> domain)
	{
		return reducerSupplier.apply(domain);
	}

	@Override
	public String toString()
	{
		return super.toString().toLowerCase();
	}
    
	private static ScalarValue<?, ?, ?, ?> sum(SerDoubleSumAvgCount count)
	{
		boolean isIntegerResult = count.isIntegerResult();
		
		if (count.getCount() == 0)
			return isIntegerResult ? IntegerValue.of((Long) null) : createNumberValue((Number) null);
		else if (isIntegerResult && count.getCountDouble() == 0)
			return IntegerValue.of(count.getLongSum());
		else 
			return createNumberValue(isUseBigDecimal() 
					? count.getBigDecimalSum().add(new BigDecimal(count.getLongSum())) 
					: count.getDoubleSum() + count.getLongSum(), NUMBERDS);
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
