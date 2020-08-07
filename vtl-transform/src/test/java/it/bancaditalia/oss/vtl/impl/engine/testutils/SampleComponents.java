/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.engine.testutils;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static java.lang.Double.NaN;
import static java.util.stream.Collectors.toList;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightDataSet;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;

public enum SampleComponents
{
	MEASURE_NUMBER_1, MEASURE_NUMBER_2, MEASURE_NUMBER_3, MEASURE_NUMBER_4,
	MEASURE_INTEGER_1, MEASURE_INTEGER_2, MEASURE_INTEGER_3, MEASURE_INTEGER_4, 
	MEASURE_STRING_1, MEASURE_STRING_2, MEASURE_STRING_3, MEASURE_STRING_4, 
	MEASURE_BOOLEAN_1, MEASURE_BOOLEAN_2, MEASURE_BOOLEAN_3, MEASURE_BOOLEAN_4, 
	IDENT_NUMBER_1, IDENT_NUMBER_2, IDENT_NUMBER_3, IDENT_NUMBER_4, 
	IDENT_INTEGER_1, IDENT_INTEGER_2, IDENT_INTEGER_3, IDENT_INTEGER_4, 
	IDENT_STRING_1, IDENT_STRING_2, IDENT_STRING_3, IDENT_STRING_4, 
	IDENT_BOOLEAN_1, IDENT_BOOLEAN_2, IDENT_BOOLEAN_3, IDENT_BOOLEAN_4, 
	ATTRIB_NUMBER_1, ATTRIB_NUMBER_2, ATTRIB_NUMBER_3, ATTRIB_NUMBER_4, 
	ATTRIB_INTEGER_1, ATTRIB_INTEGER_2, ATTRIB_INTEGER_3, ATTRIB_INTEGER_4, 
	ATTRIB_STRING_1, ATTRIB_STRING_2, ATTRIB_STRING_3, ATTRIB_STRING_4, 
	ATTRIB_BOOLEAN_1, ATTRIB_BOOLEAN_2, ATTRIB_BOOLEAN_3, ATTRIB_BOOLEAN_4;
	
	private enum DataPointSamples
	{
		INTEGER_DATAPOINTS_1(new Long[] { 1L, 2L, 3L, null, 5L, 6L }), 
		NUMBER_DATAPOINTS_1(new Double[] { 1.1, 2.2, 3.3, null, 5.5, NaN }), 
		STRING_DATAPOINTS_1(new String[] { "A", "B", "C", "D", "E", "F" }),
		BOOLEAN_DATAPOINTS_1(new Boolean[] { true, false, true, false, true, false }), 
		INTEGER_DATAPOINTS_2(new Long[] { 11L, null, 13L, 14L, 15L, 16L }),
		NUMBER_DATAPOINTS_2(new Double[] { 11.1, null, 13.3, 14.4, 15.5, 16.6 }), 
		STRING_DATAPOINTS_2(new String[] { "H", "I", "J", "K", "L", "M" }),
		BOOLEAN_DATAPOINTS_2(new Boolean[] { true, true, null, false, false, true }), 
		INTEGER_DATAPOINTS_3(new Long[] { 21L, 22L, 23L, 24L, 25L, 26L }),
		NUMBER_DATAPOINTS_3(new Double[] { 21.1, 22.2, 23.3, 24.4, 25.5, 26.6 }), 
		STRING_DATAPOINTS_3(new String[] { "A", "C", "E", "G", "I", "K" }),
		BOOLEAN_DATAPOINTS_3(new Boolean[] { true, false, false, false, true, null }),
		NUMBER_DATAPOINTS_4(new Double[] { 21.1, 22.2, 23.3, 24.4, 25.5, 26.6 }), 
		STRING_DATAPOINTS_4(new String[] { "K", "C", "G", null, "A", "E" }),
		BOOLEAN_DATAPOINTS_4(new Boolean[] { true, false, false, false, true, null });
		
		private Object[] values;
		
		private DataPointSamples(Object values[])
		{
			this.values = values; 
		}
		
		public Object[] getValues()
		{
			return values;
		}
	}
	
	private enum Wrapper
	{
		INTEGER_WRAPPER((Function<Object, ? extends ScalarValue<?, ?, ?>>) v -> v == null ? NullValue.instance(INTEGERDS) : new IntegerValue((Long) v)),
		NUMBER_WRAPPER((Function<Object, ? extends ScalarValue<?, ?, ?>>) v -> v == null ? NullValue.instance(NUMBERDS) : new DoubleValue((Double) v)),
		STRING_WRAPPER((Function<Object, ? extends ScalarValue<?, ?, ?>>) v -> v == null ? NullValue.instance(STRINGDS) : new StringValue((String) v)),
		BOOLEAN_WRAPPER((Function<Object, ? extends ScalarValue<?, ?, ?>>) v -> v == null ? NullValue.instance(BOOLEANDS) : BooleanValue.of((Boolean) v));
		
		Function<? super Object, ? extends ScalarValue<?, ?, ?>> wrapper;
		
		private Wrapper(Function<? super Object, ? extends ScalarValue<?, ?, ?>> wrapper)
		{
			this.wrapper = wrapper;
		}
		
		public Function<? super Object, ? extends ScalarValue<?, ?, ?>> getWrapper()
		{
			return wrapper;
		}
	}

	private DataStructureComponentImpl<?, ?, ?> cache;
	
	public DataStructureComponent<?, ?, ?> getComponent()
	{
		if (cache != null)
			return cache;
		
		String elem[] = toString().split("_");
		Class<? extends ComponentRole> role = null;
		switch (elem[0])
		{
			case "MEASURE": role = Measure.class; break;
			case "ATTRIB": role = Attribute.class; break;
			case "IDENT": role = Identifier.class; break;
		}
		
		ValueDomainSubset<?> domain = null;
		switch (elem[1])
		{
			case "NUMBER": domain = NUMBERDS; break;
			case "INTEGER": domain = INTEGERDS; break;
			case "STRING": domain = STRINGDS; break;
			case "BOOLEAN": domain = BOOLEANDS; break;
		}
		
		return cache = new DataStructureComponentImpl<>(elem[1]/*"VAR" + VARN.getAndIncrement()*/, role, domain);
	}
	
	public List<ScalarValue<?, ?, ?>> getValues()
	{
		try
		{
			String elem[] = toString().split("_");
			Object values[] = DataPointSamples.valueOf(elem[1] + "_DATAPOINTS_" + elem[2]).getValues();
			Function<? super Object, ? extends ScalarValue<?, ?, ?>> wrapper = Wrapper.valueOf(elem[1] + "_WRAPPER").getWrapper();
			
			return Arrays.stream(values)
					.map(wrapper)
					.collect(toList());
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
	
	public static List<DataStructureComponent<?, ?, ?>> createStructure(SampleComponents... components)
	{
		Map<String, AtomicInteger> counts = new HashMap<>(); 
		
		for (SampleComponents component: components)
			counts.put(component.getComponent().getName(), new AtomicInteger(1));
		
		return Arrays.stream(components)
			.map(SampleComponents::getComponent)
			.map(c -> c.rename(c.getName() + "_" + counts.get(c.getName()).getAndIncrement()))
			.collect(Collectors.toList());
	}
	
	private static DataSet createSample(String alias, SampleComponents components[])
	{
		List<DataStructureComponent<?,?,?>> structure = createStructure(components);
		
		return new LightDataSet(new DataStructureBuilder(structure).build(), () -> IntStream.range(0, 6)
				.mapToObj(i -> IntStream.range(0, components.length)
						.mapToObj(ci -> components[ci].getValues().stream()
								.map(v -> new SimpleEntry<>(structure.get(ci), v))
								.collect(Collectors.toList()))
						.map(l -> l.get(i))
						.collect(Collectors.toMap(Entry::getKey, Entry::getValue)))
				.map(DataPointBuilder::new)
				.map(builder -> builder.build(structure)));
	}
	
	public static DataSet getSample1()
	{
		return createSample("sample1", new SampleComponents[] { IDENT_STRING_1, IDENT_BOOLEAN_1, MEASURE_NUMBER_1, MEASURE_INTEGER_1});
	}

	public static DataSet getSample2()
	{
		return createSample("sample2", new SampleComponents[] { IDENT_STRING_1, MEASURE_NUMBER_2, MEASURE_INTEGER_2 });
	}

	public static DataSet getSample3()
	{
		return createSample("sample3", new SampleComponents[] { IDENT_STRING_1, MEASURE_BOOLEAN_2 });
	}

	public static DataSet getSample4()
	{
		return createSample("sample4", new SampleComponents[] { IDENT_STRING_1, MEASURE_BOOLEAN_3 });
	}

	public static DataSet getSample5(String domain, int level)
	{
		return createSample("sample5", new SampleComponents[] { IDENT_STRING_1, SampleComponents.valueOf("MEASURE_" + domain + "_" + level) });
	}
}
