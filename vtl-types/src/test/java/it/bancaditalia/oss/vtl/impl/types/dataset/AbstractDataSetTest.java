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
package it.bancaditalia.oss.vtl.impl.types.dataset;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireStringDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.util.SerBiPredicate;

public class AbstractDataSetTest
{
	private static final DataStructureComponent<Identifier, EntireStringDomainSubset, StringDomain> STR_ID = new ComponentMock<>("str_id", Identifier.class, Domains.STRINGDS);
	private static final DataStructureComponent<Identifier, EntireIntegerDomainSubset, IntegerDomain> INT_ID = new ComponentMock<>("int_id", Identifier.class, INTEGERDS);
	private static final DataStructureComponent<Measure, EntireIntegerDomainSubset, IntegerDomain> INT_ME = new ComponentMock<>("int_me", Measure.class, INTEGERDS);
	private static final DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> BOL_ME = new ComponentMock<>("bol_me", Measure.class, BOOLEANDS);
	private static final DataSetMetadata STRUCTURE = new DataStructureBuilder(Set.of(STR_ID, INT_ID, INT_ME, BOL_ME)).build();
	private static final String STR_ID_VAL[] = { "A", "A", "B", "B", "C" }; 
	private static final Long INT_ID_VAL[] = { 1L, 2L, 1L, 3L, 2L }; 
	private static final Long INT_ME_VAL[] = { 5L, 7L, null, 8L, 4L }; 
	private static final Boolean BOL_ME_VAL[] = { true, null, true, false, false };

	private static AbstractDataSet INSTANCE; 
	private static DataPoint DATAPOINTS[] = new DataPoint[5]; 

	@BeforeAll
	public static void beforeAll()
	{
		System.setProperty("vtl.sequential", "true");
		Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>[]> values = new HashMap<>();
		values.put(STR_ID, Arrays.stream(STR_ID_VAL).map(StringValue::of).collect(toList()).toArray(new StringValue[0]));
		values.put(INT_ID, Arrays.stream(INT_ID_VAL).map(IntegerValue::of).collect(toList()).toArray(new IntegerValue[0]));
		values.put(INT_ME, Arrays.stream(INT_ME_VAL).map(v -> (ScalarValue<?, ?, ?, ?>) (v == null ? NullValue.instance(INTEGERDS) : IntegerValue.of(v))).collect(toList()).toArray(new ScalarValue[0]));
		values.put(BOL_ME, Arrays.stream(BOL_ME_VAL).map(v -> (ScalarValue<?, ?, ?, ?>) (v == null ? NullValue.instance(BOOLEANDS) : BooleanValue.of(v))).collect(toList()).toArray(new ScalarValue[0]));
		for (int i = 0; i < 5; i++)
			DATAPOINTS[i] = new DataPointBuilder()
				.add(STR_ID, values.get(STR_ID)[i])
				.add(INT_ID, values.get(INT_ID)[i])
				.add(INT_ME, values.get(INT_ME)[i])
				.add(BOL_ME, values.get(BOL_ME)[i])
				.build(mock(LineageNode.class), STRUCTURE);
		
		INSTANCE = mock(AbstractDataSet.class, withSettings()
		        .useConstructor(STRUCTURE)
		        .defaultAnswer(CALLS_REAL_METHODS));
		when(INSTANCE.streamDataPoints()).then(i -> Arrays.stream(DATAPOINTS));
	}

	@Test
	void testMembership()
	{
		Stream<Entry<DataStructureComponent<?, ?, ?>, Stream<DataStructureComponent<?, ?, ?>>>> expected = Stream.of(
				new SimpleEntry<>(STR_ID, Stream.of(STR_ID, INT_ID, new ComponentMock<>(Measure.class, STRINGDS))),
				new SimpleEntry<>(INT_ID, Stream.of(STR_ID, INT_ID, new ComponentMock<>(Measure.class, INTEGERDS))), 
				new SimpleEntry<>(INT_ME, Stream.of(STR_ID, INT_ID, INT_ME)),
				new SimpleEntry<>(BOL_ME, Stream.of(STR_ID, INT_ID, BOL_ME)));
		
		expected.map(keepingKey(s -> s.reduce(new DataStructureBuilder(), DataStructureBuilder::addComponent, DataStructureBuilder::merge).build()))
				.map(e -> new SimpleEntry<>(e.getValue(), INSTANCE.membership(e.getKey().getVariable().getAlias()).getMetadata()))
				.forEach(e -> assertEquals(e.getKey(), e.getValue(), "Structural mismatch in membership"));
		
		verify(INSTANCE, times(4)).membership(any(VTLAlias.class));
	}

	@Test
	void testGetMetadata()
	{
		assertEquals(STRUCTURE, INSTANCE.getMetadata(), "Structural mismatch");
	}

	@Test
	void testFilteredMappedJoin()
	{
		SerBiPredicate<DataPoint, DataPoint> filter = (dp1, dp2) -> {
			assertEquals(dp1, dp2, "Joining unrelated datapoints");
			return true;
		};
		DataSet result = INSTANCE.filteredMappedJoin(STRUCTURE, INSTANCE, filter, (a, b) -> a, false);
		assertEquals(STRUCTURE, result.getMetadata());
		assertEquals(new HashSet<>(Arrays.asList(DATAPOINTS)), result.stream().collect(toSet()));
	}

	@Test
	void testMapKeepingKeys()
	{
		DataSet result = INSTANCE.mapKeepingKeys(STRUCTURE, x -> mock(Lineage.class), dp -> dp.getValues(NonIdentifier.class));
		assertEquals(STRUCTURE, result.getMetadata());
		assertEquals(new HashSet<>(Arrays.asList(DATAPOINTS)), result.stream().collect(toSet()));
	}

	@Test
	void testFilter()
	{
		DataSet result = INSTANCE.filter(dp -> true, identity());
		assertEquals(STRUCTURE, result.getMetadata());
		assertEquals(new HashSet<>(Arrays.asList(DATAPOINTS)), result.stream().collect(toSet()));
	}
}
