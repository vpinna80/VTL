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

import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.getAveragingCollector;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.getSummingCollector;
import static it.bancaditalia.oss.vtl.impl.types.operators.MedianCollector.medianCollector;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.counting;
import static it.bancaditalia.oss.vtl.util.SerCollectors.filtering;
import static it.bancaditalia.oss.vtl.util.SerCollectors.firstValue;
import static it.bancaditalia.oss.vtl.util.SerCollectors.lastValue;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.maxBy;
import static it.bancaditalia.oss.vtl.util.SerCollectors.minBy;
import static it.bancaditalia.oss.vtl.util.SerPredicate.not;
import static java.util.stream.Collector.Characteristics.UNORDERED;

import java.util.EnumSet;

import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;

public enum AnalyticOperator  
{
	COUNT(domain -> collectingAndThen(counting(), IntegerValue::of)),
	SUM(domain -> getSummingCollector(domain)), 
	AVG(domain -> getAveragingCollector(domain)),
	MEDIAN(domain -> collectingAndThen(filtering(not(NullValue.class::isInstance), medianCollector(domain.getValueClass())), opt -> opt.orElse(NullValue.unqualifiedInstance(domain)))),
	MIN(domain -> collectingAndThen(minBy(domain.getValueClass(), ScalarValue::compareTo), opt -> opt.orElse(NullValue.unqualifiedInstance(domain)))),
	MAX(domain -> collectingAndThen(maxBy(domain.getValueClass(), ScalarValue::compareTo), opt -> opt.orElse(NullValue.unqualifiedInstance(domain)))),
	VAR_POP(domain -> collectingAndThen(mapping(v -> (Number) v.get(), varianceCollector(acu -> acu[2] / (acu[0] + 1))), NumberValueImpl::createNumberValue)),
	VAR_SAMP(domain -> collectingAndThen(mapping(v -> (Number) v.get(), varianceCollector(acu -> acu[2] / acu[0])), NumberValueImpl::createNumberValue)),
	STDDEV_POP(domain -> collectingAndThen(VAR_POP.getReducer(NUMBERDS), dv -> createNumberValue(Math.sqrt((Double) dv.get())))),
	STDDEV_SAMP(domain -> collectingAndThen(VAR_SAMP.getReducer(domain), dv -> createNumberValue(Math.sqrt((Double) dv.get())))),
	FIRST_VALUE(domain -> collectingAndThen(firstValue(domain.getValueClass()), opt -> opt.orElse(NullValue.unqualifiedInstance(domain)))),
	LAST_VALUE(domain -> collectingAndThen(lastValue(domain.getValueClass()), opt -> opt.orElse(NullValue.unqualifiedInstance(domain))));

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

	private final SerFunction<ValueDomainSubset<?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>>> supplierReducer;

	private AnalyticOperator(SerFunction<ValueDomainSubset<?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>>> supplierReducer)
	{
		this.supplierReducer = supplierReducer;
	}

	public <S extends ValueDomainSubset<S, D>, D extends ValueDomain> SerCollector<ScalarValue<?, ?, ?, ?>, ?, ScalarValue<?, ?, ?, ?>> getReducer(ValueDomainSubset<S, D> domain)
	{
		return supplierReducer.apply(domain);
	}

	@Override
	public String toString()
	{
		return super.toString().toLowerCase();
	}
}