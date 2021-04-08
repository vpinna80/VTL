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
package it.bancaditalia.oss.vtl.impl.types.testutils;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public class SampleValues
{
	private static final Map<String, Object[][]> VALUES = new HashMap<>();
	private static final Map<String, Function<? super Object, ? extends ScalarValue<?, ?, ?, ?>>> WRAPPERS = new HashMap<>(); 
	
	static {
		VALUES.put("INTEGER", new Long[][] {
			new Long[] { 1L, 2L, 3L, null, 5L, 6L },
			new Long[] { 11L, null, 13L, 14L, 15L, 16L },
			new Long[] { 21L, 22L, 23L, 24L, 25L, 26L }
		});
		VALUES.put("NUMBER", new Double[][] {
			new Double[] { 1.1, 2.2, 3.3, null, 5.5, Double.NaN },
			new Double[] { 11.1, null, 13.3, 14.4, 15.5, 16.6 },
			new Double[] { 21.1, 22.2, 23.3, 24.4, 25.5, 26.6 },
		});
		VALUES.put("STRING", new String[][] {
			new String[] { "A", "B", "C", "D", "E", "F" },
			new String[] { "H", "I", "J", "K", "L", "M" },
			new String[] { "A", "C", "E", "G", "I", "K" },
			new String[] { "K", "C", "G", null, "A", "E" }
		});
		VALUES.put("BOOLEAN", new Boolean[][] {
			new Boolean[] { true, false, true, false, true, false },
			new Boolean[] { true, true, null, false, false, true },
			new Boolean[] { true, false, false, false, true, null },
			new Boolean[] { true, false, false, false, true, null }
		});

		WRAPPERS.put("INTEGER", v -> v == null ? NullValue.instance(INTEGERDS) : IntegerValue.of((Long) v));
		WRAPPERS.put("NUMBER", v -> v == null ? NullValue.instance(NUMBERDS) : DoubleValue.of((Double) v));
		WRAPPERS.put("STRING", v -> v == null ? NullValue.instance(STRINGDS) : StringValue.of((String) v));
		WRAPPERS.put("BOOLEAN", v -> v == null ? NullValue.instance(BOOLEANDS) : BooleanValue.of((Boolean) v));
	}

	private SampleValues() { }
	
	public static List<ScalarValue<?, ?, ?, ?>> getValues(String type, int index)
	{
		Object values[] = VALUES.get(type)[index - 1];
		Function<? super Object, ? extends ScalarValue<?, ?, ?, ?>> wrapper = WRAPPERS.get(type);
		
		return Arrays.stream(values)
				.map(wrapper)
				.collect(Collectors.toList());
	}
}
