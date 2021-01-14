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
package it.bancaditalia.oss.vtl.impl.environment.dataset;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

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
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
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
}
