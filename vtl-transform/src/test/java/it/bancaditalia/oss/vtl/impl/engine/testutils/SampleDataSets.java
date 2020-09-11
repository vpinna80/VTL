package it.bancaditalia.oss.vtl.impl.engine.testutils;

import static it.bancaditalia.oss.vtl.impl.engine.testutils.SampleValues.VAR_SAMPLE_LEN;
import static it.bancaditalia.oss.vtl.impl.engine.testutils.SampleVariables.IDENT_BOOLEAN_1;
import static it.bancaditalia.oss.vtl.impl.engine.testutils.SampleVariables.IDENT_STRING_1;
import static it.bancaditalia.oss.vtl.impl.engine.testutils.SampleVariables.MEASURE_BOOLEAN_2;
import static it.bancaditalia.oss.vtl.impl.engine.testutils.SampleVariables.MEASURE_BOOLEAN_3;
import static it.bancaditalia.oss.vtl.impl.engine.testutils.SampleVariables.MEASURE_INTEGER_1;
import static it.bancaditalia.oss.vtl.impl.engine.testutils.SampleVariables.MEASURE_INTEGER_2;
import static it.bancaditalia.oss.vtl.impl.engine.testutils.SampleVariables.MEASURE_NUMBER_1;
import static it.bancaditalia.oss.vtl.impl.engine.testutils.SampleVariables.MEASURE_NUMBER_2;
import static it.bancaditalia.oss.vtl.util.Utils.entriesToMap;
import static java.util.stream.Collectors.toList;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightDataSet;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public enum SampleDataSets implements DataSet
{
	SAMPLE1(new SampleVariables[] { IDENT_STRING_1, IDENT_BOOLEAN_1, MEASURE_NUMBER_1, MEASURE_INTEGER_1}),
	SAMPLE2(new SampleVariables[] { IDENT_STRING_1, MEASURE_NUMBER_2, MEASURE_INTEGER_2 }),
	SAMPLE3(new SampleVariables[] { IDENT_STRING_1, MEASURE_BOOLEAN_2 }),
	SAMPLE4(new SampleVariables[] { IDENT_STRING_1, MEASURE_BOOLEAN_3 });

	private final DataSet dataset;

	private SampleDataSets(SampleVariables components[])
	{
		dataset = createSample(components);
	}

	public static DataSetMetadata createStructure(SampleVariables... components)
	{
		Map<String, AtomicInteger> counts = new HashMap<>(); 
		
		for (SampleVariables component: components)
			counts.put(component.getComponent().getName(), new AtomicInteger(1));
		
		return Arrays.stream(components)
			.map(SampleVariables::getComponent)
			.map(c -> c.rename(c.getName() + "_" + counts.get(c.getName()).getAndIncrement()))
			.collect(DataStructureBuilder.toDataStructure());
	}
	
	private static DataSet createSample(SampleVariables components[])
	{
		DataSetMetadata structure = createStructure(components);
		
		return new LightDataSet(structure, () -> IntStream.range(0, VAR_SAMPLE_LEN)
				.mapToObj(i -> IntStream.range(0, components.length)
						.mapToObj(ci -> SampleValues.getValues(components[ci].getType(), components[ci].getIndex()).stream()
								.map(v -> new SimpleEntry<>(components[ci].getComponent(), v))
								.collect(toList()))
						.map(l -> l.get(i))
						.collect(entriesToMap()))
				.map(DataPointBuilder::new)
				.map(builder -> builder.build(structure)));
	}

	public static DataSet getCustomSample(String domain, int level)
	{
		return createSample(new SampleVariables[] { IDENT_STRING_1, SampleVariables.valueOf("MEASURE_" + domain + "_" + level) });
	}

	public Stream<DataPoint> stream()
	{
		return dataset.stream();
	}

	public DataSetMetadata getMetadata()
	{
		return dataset.getMetadata();
	}

	public DataSet membership(String component)
	{
		return dataset.membership(component);
	}

	public Optional<DataStructureComponent<?, ?, ?>> getComponent(String name)
	{
		return dataset.getComponent(name);
	}

	public DataSet getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> keyValues)
	{
		return dataset.getMatching(keyValues);
	}

	public DataSet filteredMappedJoin(DataSetMetadata metadata, DataSet rightDataset, BiPredicate<DataPoint, DataPoint> filter,
			BinaryOperator<DataPoint> mergeOp)
	{
		return dataset.filteredMappedJoin(metadata, rightDataset, filter, mergeOp);
	}

	public DataSet filter(Predicate<DataPoint> predicate)
	{
		return dataset.filter(predicate);
	}

	public DataSet mapKeepingKeys(DataSetMetadata metadata,
			Function<? super DataPoint, ? extends Map<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>, ? extends ScalarValue<?, ?, ?>>> operator)
	{
		return dataset.mapKeepingKeys(metadata, operator);
	}

	@Override
	public <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys,
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> filter, Collector<DataPoint, A, TT> groupCollector,
			BiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, T> finisher)
	{
		return dataset.streamByKeys(keys, filter, groupCollector, finisher);
	}
}
