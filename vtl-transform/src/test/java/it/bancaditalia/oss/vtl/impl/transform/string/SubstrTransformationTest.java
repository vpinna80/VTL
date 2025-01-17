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
package it.bancaditalia.oss.vtl.impl.transform.string;

import static it.bancaditalia.oss.vtl.impl.data.samples.SampleDataSets.SAMPLE13;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.transform.ConstantOperand;
import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class SubstrTransformationTest
{
	public static Stream<Arguments> test()
	{
		// "Filled String", "    Leading spaces", "Trailing spaces    ", "    Leading and trailing     ", "\"Quoted\" 'String'", "\t\b \n\r\f"
		return Stream.of(
				Arguments.of(SAMPLE13, null, null, new String[] { "Filled String", "    Leading spaces", "Trailing spaces    ", "    Leading and trailing     ", "\"Quoted\" 'String'", "\t\b \n\r\f" }),
				Arguments.of(SAMPLE13, null,   5L, new String[] { "Fille",         "    L",              "Trail",               "    L",                         "\"Quot",              "\t\b \n\r" }),
				Arguments.of(SAMPLE13,  13L, null, new String[] { "g",             "spaces",             "ces    ",             "and trailing     ",             "ring'",               "" }),
				Arguments.of(SAMPLE13,  13L,   5L, new String[] { "g",             "space",              "ces  ",               "and t",                         "ring'",               "" })
		);
	}
	
	@ParameterizedTest(name = "substr({0}, {1}, {2})")
	@MethodSource
	public void test(DataSet dataset, Long startV, Long lengthV, String[] expected)
	{
		VarIDOperand ds = new VarIDOperand(VTLAliasImpl.of("left"));
		ConstantOperand start = new ConstantOperand(IntegerValue.of(startV));
		ConstantOperand length = new ConstantOperand(IntegerValue.of(lengthV));
		Map<VTLAlias, VTLValue> map = new HashMap<>();
		map.put(VTLAliasImpl.of("left"), dataset);
		TransformationScheme session = TestUtils.mockSession(map);

		SubstrTransformation substrTransformation = new SubstrTransformation(ds, start, length);
		
		DataSetMetadata metadata = (DataSetMetadata) substrTransformation.getMetadata(session);
		assertTrue(metadata.contains(VTLAliasImpl.of("string_2")));
		
		DataSet computedResult = (DataSet) substrTransformation.eval(session);
		Optional<DataStructureComponent<Identifier, ?, ?>> oId = metadata.getComponent(VTLAliasImpl.of("string_1"), Identifier.class, STRINGDS);		
		Optional<DataStructureComponent<Measure, ?, ?>> oMeasure = metadata.getComponent(VTLAliasImpl.of("string_2"), Measure.class, STRINGDS);
		
		assertTrue(oId.isPresent(), "String id present");
		assertTrue(oMeasure.isPresent(), "String measure is present");
		
		DataStructureComponent<Identifier, ?, ?> id = oId.get();
		DataStructureComponent<Measure, ?, ?> resultMeasure = oMeasure.get();
		
		computedResult.stream()
			.forEach(dp -> assertEquals(expected[dp.get(id).get().toString().charAt(0) - 'A'], dp.get(resultMeasure).get(), 
					"For result n. " + (int)(dp.get(id).get().toString().charAt(0) - 'A')));
	}
}
