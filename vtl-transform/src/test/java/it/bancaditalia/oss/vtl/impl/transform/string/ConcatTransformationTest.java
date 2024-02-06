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
import static it.bancaditalia.oss.vtl.impl.data.samples.SampleDataSets.SAMPLE14;
import static it.bancaditalia.oss.vtl.impl.data.samples.SampleDataSets.SAMPLE15;
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

import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class ConcatTransformationTest
{
	public static Stream<Arguments> test()
	{
		// "Filled String", "    Leading spaces", "Trailing spaces    ", "    Leading and trailing     ", "\"Quoted\" 'String'", "\t\b \n\r\f"
		return Stream.of(
				Arguments.of(SAMPLE13, SAMPLE14, new String[] { "Filled StringA", "    Leading spacesC", "Trailing spaces    E", "    Leading and trailing     G", "\"Quoted\" 'String'I", "\t\b \n\r\fK" }),
				Arguments.of(SAMPLE13, SAMPLE15, new String[] { "Filled StringK", "    Leading spacesC", "Trailing spaces    G", null, "\"Quoted\" 'String'A", "\t\b \n\r\fE" }),
				Arguments.of(SAMPLE14, SAMPLE15, new String[] { "AK", "CC", "EG", null, "IA", "KE" })
		);
	}
	
	@ParameterizedTest(name = "{0} || {1}")
	@MethodSource
	public void test(DataSet leftDS, DataSet rightDS, String[] result)
	{
		VarIDOperand left = new VarIDOperand("left"), right = new VarIDOperand("right");
		Map<String, DataSet> map = new HashMap<>();
		map.put("left", leftDS);
		map.put("right", rightDS);
		TransformationScheme session = TestUtils.mockSession(map);

		ConcatTransformation coTransformation = new ConcatTransformation(left, right);
		
		DataSetMetadata metadata = (DataSetMetadata) coTransformation.getMetadata(session);
		assertTrue(metadata.contains("string_2"));
		
		DataSet computedResult = (DataSet) coTransformation.eval(session);
		Optional<DataStructureComponent<Identifier, ?, ?>> oId = metadata.getComponent("string_1", Identifier.class, STRINGDS);		
		Optional<DataStructureComponent<Measure, ?, ?>> oMeasure = metadata.getComponent("string_2", Measure.class, STRINGDS);
		
		assertTrue(oId.isPresent(), "String id present");
		assertTrue(oMeasure.isPresent(), "String measure present");
		
		DataStructureComponent<Identifier, ?, ?> id = oId.get();
		DataStructureComponent<Measure, ?, ?> res = oMeasure.get();
		
		DataSet leftD = (DataSet) left.eval(session), 
				rightD = (DataSet) right.eval(session);
		
		computedResult.stream()
			.forEach(dp -> assertEquals(result[dp.get(id).get().toString().charAt(0) - 'A'], dp.get(res).get(), 
					dp.get(id).get().toString() + "(" + leftD.stream()
						.filter(dpl -> dpl.get(id).equals(dp.get(id)))
						.map(dpl -> dpl.getValues(Measure.class).values().iterator().next().toString())
						.findFirst()
						.get() + " && " + 
					rightD.stream()
						.filter(dpr -> dpr.get(id).equals(dp.get(id)))
						.map(dpr -> dpr.getValues(Measure.class).values().iterator().next().toString())
						.findFirst()
						.get() + ")"));
	}
}
