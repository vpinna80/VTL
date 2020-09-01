package it.bancaditalia.oss.vtl.impl.environment.dataset;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.util.Utils.entriesToMap;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;

public class ColumnarDataSetTest
{
	private static final DataStructureComponent<Identifier, StringDomainSubset, StringDomain> STR_ID = new DataStructureComponentImpl<>("STR_ID", Identifier.class, Domains.STRINGDS);
	private static final DataStructureComponent<Identifier, IntegerDomainSubset, IntegerDomain> INT_ID = new DataStructureComponentImpl<>("INT_ID", Identifier.class, INTEGERDS);
	private static final DataStructureComponent<Measure, IntegerDomainSubset, IntegerDomain> INT_ME = new DataStructureComponentImpl<>("INT_ME", Measure.class, INTEGERDS);
	private static final DataStructureComponent<Measure, BooleanDomainSubset, BooleanDomain> BOL_ME = new DataStructureComponentImpl<>("BOL_ME", Measure.class, BOOLEANDS);
	private static final VTLDataSetMetadata STRUCTURE = new DataStructureBuilder(STR_ID, INT_ID, INT_ME, BOL_ME).build();
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
	
	private ColumnarDataSet INSTANCE; 
	private DataPoint DATAPOINTS[] = new DataPoint[5]; 

	@BeforeEach
	public void beforeEach()
	{
		Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>[]> values = new HashMap<>();
		values.put(STR_ID, arrayToArray(StringValue::new, STR_ID_VAL));
		values.put(INT_ID, arrayToArray(IntegerValue::new, INT_ID_VAL));
		values.put(INT_ME, arrayToArray(v -> v == null ? NullValue.instance(INTEGERDS) : new IntegerValue(v), INT_ME_VAL));
		values.put(BOL_ME, arrayToArray(v -> v == null ? NullValue.instance(BOOLEANDS) : BooleanValue.of(v), BOL_ME_VAL));
		INSTANCE = new ColumnarDataSet(values);
		for (int i = 0; i < 5; i++)
			DATAPOINTS[i] = new DataPointBuilder()
				.add(STR_ID, values.get(STR_ID)[i])
				.add(INT_ID, values.get(INT_ID)[i])
				.add(INT_ME, values.get(INT_ME)[i])
				.add(BOL_ME, values.get(BOL_ME)[i])
				.build(STRUCTURE);
	}
	
	@Test
	void testStreamDataPoints()
	{
		boolean found[] = new boolean[5];
		Iterator<DataPoint> it = INSTANCE.stream().iterator();
		while (it.hasNext())
		{
			DataPoint dp = it.next();
			for (int i = 0; i < 5; i++)
				if (new StringValue(STR_ID_VAL[i]).equals(dp.get(STR_ID)) && new IntegerValue(INT_ID_VAL[i]).equals(dp.get(INT_ID)))
				{
					ScalarValue<?, ?, ?> intVal = INT_ME_VAL[i] == null ? NullValue.instance(INTEGERDS) : new IntegerValue(INT_ME_VAL[i]);
					ScalarValue<?, ?, ?> bolVal = BOL_ME_VAL[i] == null ? NullValue.instance(BOOLEANDS) : BooleanValue.of(BOL_ME_VAL[i]);
					assertEquals(intVal, dp.get(INT_ME));
					assertEquals(bolVal, dp.get(BOL_ME));
					found[i] = true;
				}
		}
		for (int i = 1; i < 5; i++)
			assertTrue(found[i], "Datapoint " + i + " not found");
	}

	@Test
	void testGetMatching()
	{
		ScalarValue<?, ?, ?>[] STR_ID_SC = arrayToArray(StringValue::new, STR_ID_VAL);
		ScalarValue<?, ?, ?>[] INT_ID_SC = arrayToArray(IntegerValue::new, INT_ID_VAL);
		
		@SuppressWarnings("unchecked")
		Entry<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, Set<DataPoint>> matches[] = 
				(Entry<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, Set<DataPoint>>[]) new Entry[3];
		
		// STR_ID = "A"
		matches[0] = new SimpleEntry<>(singletonMap(STR_ID, STR_ID_SC[0]), Stream.of(DATAPOINTS[0], DATAPOINTS[1]).collect(toSet()));
		// INT_ID = 2L
		matches[1] = new SimpleEntry<>(singletonMap(INT_ID, INT_ID_SC[1]), Stream.of(DATAPOINTS[1], DATAPOINTS[4]).collect(toSet()));
		// STR_ID = "A" && INT_ID = 3L
		matches[2] = new SimpleEntry<>(Stream.of(new SimpleEntry<>(STR_ID, STR_ID_SC[0]), new SimpleEntry<>(INT_ID, INT_ID_SC[3]))
				.collect(entriesToMap()), emptySet());

		for (int i = 0; i < matches.length; i++)
			assertEquals(matches[i].getValue(), INSTANCE.getMatching(matches[i].getKey()).collect(toSet()), "Match " + i + " failed");
	}

	@Test
	void testStreamByKeys()
	{
		ScalarValue<?, ?, ?>[] STR_ID_SC = arrayToArray(StringValue::new, STR_ID_VAL);

		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, Set<DataPoint>> results = INSTANCE
			.streamByKeys(singleton(STR_ID), emptyMap(), (k, g) -> new SimpleEntry<>(k, g.collect(toSet())))
			.collect(entriesToMap());
		
		Stream.of(new SimpleEntry<>(singletonMap(STR_ID, STR_ID_SC[0]), Stream.of(DATAPOINTS[0], DATAPOINTS[1]).collect(toSet())),
				new SimpleEntry<>(singletonMap(STR_ID, STR_ID_SC[2]), Stream.of(DATAPOINTS[2], DATAPOINTS[3]).collect(toSet())),
				new SimpleEntry<>(singletonMap(STR_ID, STR_ID_SC[4]), singleton(DATAPOINTS[4])))
			.forEach(e -> assertEquals(e.getValue(), results.get(e.getKey()), "Matching " + e.getKey() + " failed"));
	}
}
