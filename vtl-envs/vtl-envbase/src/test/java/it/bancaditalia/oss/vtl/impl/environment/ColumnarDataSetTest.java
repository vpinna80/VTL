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
package it.bancaditalia.oss.vtl.impl.environment;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import it.bancaditalia.oss.vtl.impl.environment.dataset.ColumnarDataSet;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireStringDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;

public class ColumnarDataSetTest
{
	private static final DataStructureComponent<Identifier, EntireStringDomainSubset, StringDomain> STR_ID = DataStructureComponentImpl.of("STR_ID", Identifier.class, Domains.STRINGDS);
	private static final DataStructureComponent<Identifier, EntireIntegerDomainSubset, IntegerDomain> INT_ID = DataStructureComponentImpl.of("INT_ID", Identifier.class, INTEGERDS);
	private static final DataStructureComponent<Measure, EntireIntegerDomainSubset, IntegerDomain> INT_ME = DataStructureComponentImpl.of("INT_ME", Measure.class, INTEGERDS);
	private static final DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> BOL_ME = DataStructureComponentImpl.of("BOL_ME", Measure.class, BOOLEANDS);
	private static final DataSetMetadata STRUCTURE = new DataStructureBuilder(STR_ID, INT_ID, INT_ME, BOL_ME).build();
	private static final String STR_ID_VAL[] = { "A", "A", "B", "B", "C" }; 
	private static final Long INT_ID_VAL[] = { 1L, 2L, 1L, 3L, 2L }; 
	private static final Long INT_ME_VAL[] = { 5L, 7L, null, 8L, 4L }; 
	private static final Boolean BOL_ME_VAL[] = { true, null, true, false, false };
	
	private ColumnarDataSet INSTANCE; 
	private DataPoint DATAPOINTS[] = new DataPoint[5]; 

	@BeforeEach
	public void beforeEach()
	{
		Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>[]> values = new HashMap<>();
		values.put(STR_ID, Arrays.stream(STR_ID_VAL).map(StringValue::of).collect(toList()).toArray(new StringValue[0]));
		values.put(INT_ID, Arrays.stream(INT_ID_VAL).map(IntegerValue::of).collect(toList()).toArray(new IntegerValue[0]));
		values.put(INT_ME, Arrays.stream(INT_ME_VAL).map(v -> (ScalarValue<?, ?, ?, ?>) (v == null ? NullValue.instance(INTEGERDS) : IntegerValue.of(v))).collect(toList()).toArray(new ScalarValue[0]));
		values.put(BOL_ME, Arrays.stream(BOL_ME_VAL).map(v -> (ScalarValue<?, ?, ?, ?>) (v == null ? NullValue.instance(BOOLEANDS) : BooleanValue.of(v))).collect(toList()).toArray(new ScalarValue[0]));
		INSTANCE = new ColumnarDataSet("test", values);
		for (int i = 0; i < 5; i++)
			DATAPOINTS[i] = new DataPointBuilder()
				.add(STR_ID, values.get(STR_ID)[i])
				.add(INT_ID, values.get(INT_ID)[i])
				.add(INT_ME, values.get(INT_ME)[i])
				.add(BOL_ME, values.get(BOL_ME)[i])
				.build(mock(LineageNode.class), STRUCTURE);
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
				if (StringValue.of(STR_ID_VAL[i]).equals(dp.get(STR_ID)) && IntegerValue.of(INT_ID_VAL[i]).equals(dp.get(INT_ID)))
				{
					ScalarValue<?, ?, ?, ?> intVal = INT_ME_VAL[i] == null ? NullValue.instance(INTEGERDS) : IntegerValue.of(INT_ME_VAL[i]);
					ScalarValue<?, ?, ?, ?> bolVal = BOL_ME_VAL[i] == null ? NullValue.instance(BOOLEANDS) : BooleanValue.of(BOL_ME_VAL[i]);
					assertEquals(intVal, dp.get(INT_ME));
					assertEquals(bolVal, dp.get(BOL_ME));
					found[i] = true;
				}
		}
		for (int i = 1; i < 5; i++)
			assertTrue(found[i], "Datapoint " + i + " not found");
	}
}
