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
package it.bancaditalia.oss.vtl.util;

import static it.bancaditalia.oss.vtl.util.SerFunction.identity;
import static java.util.stream.Collector.Characteristics.IDENTITY_FINISH;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collector;

public class SerCollector<T, A, R> implements Collector<T, A, R>, Serializable
{
	private static final long serialVersionUID = 1L;

	private final SerSupplier<A> supplier;
	private final SerBiConsumer<A, T> accumulator;
	private final SerBinaryOperator<A> combiner;
	private final SerFunction<A, R> finisher;
	private final EnumSet<Characteristics> characteristics;

	public static <T, A, R> SerCollector<T, A, R> of(SerSupplier<A> supplier, SerBiConsumer<A, T> accumulator, SerBinaryOperator<A> combiner,
			SerFunction<A, R> finisher, Set<Characteristics> characteristics)
	{
		return new SerCollector<>(supplier, accumulator, combiner, finisher, characteristics);
	}

	public static <T, A> SerCollector<T, A, A> of(SerSupplier<A> supplier, SerBiConsumer<A, T> accumulator, SerBinaryOperator<A> combiner,
			Set<Characteristics> characteristics)
	{
		EnumSet<Characteristics> newCharacteristics = EnumSet.of(IDENTITY_FINISH);
		newCharacteristics.addAll(characteristics);
		return new SerCollector<>(supplier, accumulator, combiner, identity(), newCharacteristics);
	}

	protected SerCollector(SerSupplier<A> supplier, SerBiConsumer<A, T> accumulator, SerBinaryOperator<A> combiner,
			SerFunction<A, R> finisher, Set<Characteristics> characteristics)
	{
		this.supplier = supplier;
		this.accumulator = accumulator;
		this.combiner = combiner;
		this.finisher = finisher;
		this.characteristics = characteristics.isEmpty() ? EnumSet.noneOf(Characteristics.class) : EnumSet.copyOf(characteristics);
	}

	@Override
	public SerSupplier<A> supplier()
	{
		return supplier;
	}

	@Override
	public SerBiConsumer<A, T> accumulator()
	{
		return accumulator;
	}

	@Override
	public SerBinaryOperator<A> combiner()
	{
		return combiner;
	}

	@Override
	public SerFunction<A, R> finisher()
	{
		return finisher;
	}

	@Override
	public EnumSet<Characteristics> characteristics()
	{
		return characteristics;
	}
}
