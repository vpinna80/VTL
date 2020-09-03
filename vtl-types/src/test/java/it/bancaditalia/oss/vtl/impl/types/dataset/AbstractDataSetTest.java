package it.bancaditalia.oss.vtl.impl.types.dataset;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;

public class AbstractDataSetTest
{
	private static final DataStructureComponent<Identifier, StringDomainSubset, StringDomain> STR_ID = new DataStructureComponentImpl<>("STR_ID", Identifier.class, Domains.STRINGDS);
	private static final DataStructureComponent<Identifier, IntegerDomainSubset, IntegerDomain> INT_ID = new DataStructureComponentImpl<>("INT_ID", Identifier.class, INTEGERDS);
	private static final DataStructureComponent<Measure, IntegerDomainSubset, IntegerDomain> INT_ME = new DataStructureComponentImpl<>("INT_ME", Measure.class, INTEGERDS);
	private static final DataStructureComponent<Measure, BooleanDomainSubset, BooleanDomain> BOL_ME = new DataStructureComponentImpl<>("BOL_ME", Measure.class, BOOLEANDS);
	private static final DataSetMetadata STRUCTURE = new DataStructureBuilder(STR_ID, INT_ID, INT_ME, BOL_ME).build();
	private static final String STR_ID_VAL[] = { "A", "A", "B", "B", "C" }; 
	private static final Long INT_ID_VAL[] = { 1L, 2L, 1L, 3L, 2L }; 
	private static final Long INT_ME_VAL[] = { 5L, 7L, null, 8L, 4L }; 
	private static final Boolean BOL_ME_VAL[] = { true, null, true, false, false };

	private static <T extends ScalarValue<R, ?, ?>, R extends Comparable<?> & Serializable> ScalarValue<?, ?, ?>[] arrayToArray(Function<R, T> mapper, R[] values)
	{
		ScalarValue<?, ?, ?>[] result = new ScalarValue[values.length];
		for (int i = 0; i < values.length; i++)
			result[i] = mapper.apply(values[i]);
		return result;
	}
	
	private AbstractDataSet INSTANCE; 
	private DataPoint DATAPOINTS[] = new DataPoint[5]; 

	@BeforeEach
	public void beforeEach()
	{
		Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>[]> values = new HashMap<>();
		values.put(STR_ID, arrayToArray(StringValue::new, STR_ID_VAL));
		values.put(INT_ID, arrayToArray(IntegerValue::new, INT_ID_VAL));
		values.put(INT_ME, arrayToArray(v -> v == null ? NullValue.instance(INTEGERDS) : new IntegerValue(v), INT_ME_VAL));
		values.put(BOL_ME, arrayToArray(v -> v == null ? NullValue.instance(BOOLEANDS) : BooleanValue.of(v), BOL_ME_VAL));
		for (int i = 0; i < 5; i++)
			DATAPOINTS[i] = new DataPointBuilder()
				.add(STR_ID, values.get(STR_ID)[i])
				.add(INT_ID, values.get(INT_ID)[i])
				.add(INT_ME, values.get(INT_ME)[i])
				.add(BOL_ME, values.get(BOL_ME)[i])
				.build(STRUCTURE);
		
		INSTANCE = mock(AbstractDataSet.class, withSettings()
		        .useConstructor(STRUCTURE)
		        .defaultAnswer(CALLS_REAL_METHODS));
		when(INSTANCE.streamDataPoints()).then(i -> Arrays.stream(DATAPOINTS));
	}

	@Test
	void testMembership()
	{
		Stream<Entry<DataStructureComponent<?, ?, ?>, Stream<DataStructureComponent<?, ?, ?>>>> expected = Stream.of(
				new SimpleEntry<>(STR_ID, Stream.of(STR_ID, INT_ID, new DataStructureComponentImpl<>(STRINGDS.getVarName(), Measure.class, STRINGDS))),
				new SimpleEntry<>(INT_ID, Stream.of(STR_ID, INT_ID, new DataStructureComponentImpl<>(INTEGERDS.getVarName(), Measure.class, INTEGERDS))), 
				new SimpleEntry<>(INT_ME, Stream.of(STR_ID, INT_ID, INT_ME)),
				new SimpleEntry<>(BOL_ME, Stream.of(STR_ID, INT_ID, BOL_ME)));
		
		expected.map(keepingKey(s -> s.reduce(new DataStructureBuilder(), DataStructureBuilder::addComponent, DataStructureBuilder::merge).build()))
				.map(e -> new SimpleEntry<>(e.getValue(), INSTANCE.membership(e.getKey().getName()).getMetadata()))
				.forEach(e -> assertEquals(e.getKey(), e.getValue(), "Structural mismatch in membership"));
		
		verify(INSTANCE, times(4)).membership(anyString());
	}

	@Test
	void testGetMetadata()
	{
		assertEquals(STRUCTURE, INSTANCE.getMetadata(), "Structural mismatch");
	}

	@Test
	void testFilteredMappedJoin()
	{
		BiPredicate<DataPoint, DataPoint> filter = (dp1, dp2) -> {
			assertEquals(dp1, dp2, "Joining unrelated datapoints");
			return true;
		};
		DataSet result = INSTANCE.filteredMappedJoin(STRUCTURE, INSTANCE, filter, (a, b) -> a);
		assertEquals(STRUCTURE, result.getMetadata());
		assertEquals(new HashSet<>(Arrays.asList(DATAPOINTS)), result.stream().collect(toSet()));
	}

	@Test
	void testMapKeepingKeys()
	{
		DataSet result = INSTANCE.mapKeepingKeys(STRUCTURE, dp -> dp.getValues(NonIdentifier.class));
		assertEquals(STRUCTURE, result.getMetadata());
		assertEquals(new HashSet<>(Arrays.asList(DATAPOINTS)), result.stream().collect(toSet()));
	}

	@Test
	void testFilter()
	{
		DataSet result = INSTANCE.filter(dp -> true);
		assertEquals(STRUCTURE, result.getMetadata());
		assertEquals(new HashSet<>(Arrays.asList(DATAPOINTS)), result.stream().collect(toSet()));
	}
}
