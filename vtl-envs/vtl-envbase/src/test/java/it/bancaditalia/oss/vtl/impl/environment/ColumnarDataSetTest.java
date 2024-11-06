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

import static it.bancaditalia.oss.vtl.impl.environment.TestComponents.BOL_ME;
import static it.bancaditalia.oss.vtl.impl.environment.TestComponents.INT_ID;
import static it.bancaditalia.oss.vtl.impl.environment.TestComponents.INT_ME;
import static it.bancaditalia.oss.vtl.impl.environment.TestComponents.STR_ID;
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
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public class ColumnarDataSetTest
{
	private static final DataSetMetadata STRUCTURE = new DataStructureBuilder(STR_ID.get(), INT_ID.get(), INT_ME.get(), BOL_ME.get()).build();
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
		values.put(STR_ID.get(), Arrays.stream(STR_ID_VAL).map(StringValue::of).collect(toList()).toArray(new StringValue[0]));
		values.put(INT_ID.get(), Arrays.stream(INT_ID_VAL).map(IntegerValue::of).collect(toList()).toArray(new IntegerValue[0]));
		values.put(INT_ME.get(), Arrays.stream(INT_ME_VAL).map(v -> (ScalarValue<?, ?, ?, ?>) (v == null ? NullValue.instance(INTEGERDS) : IntegerValue.of(v))).collect(toList()).toArray(new ScalarValue[0]));
		values.put(BOL_ME.get(), Arrays.stream(BOL_ME_VAL).map(v -> (ScalarValue<?, ?, ?, ?>) (v == null ? NullValue.instance(BOOLEANDS) : BooleanValue.of(v))).collect(toList()).toArray(new ScalarValue[0]));
		INSTANCE = new ColumnarDataSet(VTLAliasImpl.of("test"), values);
		for (int i = 0; i < 5; i++)
			DATAPOINTS[i] = new DataPointBuilder()
				.add(STR_ID.get(), values.get(STR_ID.get())[i])
				.add(INT_ID.get(), values.get(INT_ID.get())[i])
				.add(INT_ME.get(), values.get(INT_ME.get())[i])
				.add(BOL_ME.get(), values.get(BOL_ME.get())[i])
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
				if (StringValue.of(STR_ID_VAL[i]).equals(dp.get(STR_ID.get())) && IntegerValue.of(INT_ID_VAL[i]).equals(dp.get(INT_ID.get())))
				{
					ScalarValue<?, ?, ?, ?> intVal = INT_ME_VAL[i] == null ? NullValue.instance(INTEGERDS) : IntegerValue.of(INT_ME_VAL[i]);
					ScalarValue<?, ?, ?, ?> bolVal = BOL_ME_VAL[i] == null ? NullValue.instance(BOOLEANDS) : BooleanValue.of(BOL_ME_VAL[i]);
					assertEquals(intVal, dp.get(INT_ME.get()));
					assertEquals(bolVal, dp.get(BOL_ME.get()));
					found[i] = true;
				}
		}
		for (int i = 1; i < 5; i++)
			assertTrue(found[i], "Datapoint " + i + " not found");
	}
}
