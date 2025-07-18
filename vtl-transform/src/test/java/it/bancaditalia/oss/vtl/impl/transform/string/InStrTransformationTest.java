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
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.INT_VAR;
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
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class InStrTransformationTest
{
	public static Stream<Arguments> test()
	{
		// "Filled String", "    Leading spaces", "Trailing spaces    ", "    Leading and trailing     ", "\"Quoted\" 'String'", "\t\b \n\r\f"
		return Stream.of(
				Arguments.of(SAMPLE13, "e", null, null, new Long[] {  5L,  6L, 14L,  6L,  6L, 0L }),
				Arguments.of(SAMPLE13, "i", null,   2L, new Long[] { 11L,  0L,  6L, 20L,  0L, 0L }),
				Arguments.of(SAMPLE13, "i",   7L, null, new Long[] { 11L,  9L,  0L,  9L, 14L, 0L }),
				Arguments.of(SAMPLE13, "i",   7L,   2L, new Long[] {  0L,  0L,  0L, 20L,  0L, 0L })
		);
	}
	
	@ParameterizedTest(name = "instr({0}, {1}, {2}, {3})")
	@MethodSource
	public void test(DataSet leftDS, String rightV, Long startV, Long occurrenceV, Long[] expected)
	{
		VarIDOperand left = new VarIDOperand(VTLAliasImpl.of("left"));
		ConstantOperand right = new ConstantOperand(StringValue.of(rightV));
		ConstantOperand start = new ConstantOperand(IntegerValue.of(startV));
		ConstantOperand occurrence = new ConstantOperand(IntegerValue.of(occurrenceV));
		Map<VTLAlias, VTLValue> map = new HashMap<>();
		map.put(VTLAliasImpl.of("left"), leftDS);
		TransformationScheme session = TestUtils.mockSession(map);

		InStrTransformation instrTransformation = new InStrTransformation(left, right, start, occurrence);
		
		DataSetStructure metadata = (DataSetStructure) instrTransformation.getMetadata(session);
		Optional<DataSetComponent<Measure, ?, ?>> int_var = metadata.getComponent(INT_VAR.getAlias(), Measure.class);
		assertTrue(int_var.isPresent());
		Optional<DataSetComponent<Identifier, ?, ?>> oId = metadata.getComponent(VTLAliasImpl.of("string_1"), Identifier.class, STRINGDS);		
		assertTrue(oId.isPresent(), "String id present");
		
		DataSet computedResult = (DataSet) instrTransformation.eval(session);
		assertEquals(metadata, computedResult.getMetadata()); 
		
		DataSetComponent<Identifier, ?, ?> id = oId.get();
		DataSetComponent<Measure, ?, ?> resultMeasure = int_var.get();
		
		computedResult.stream()
			.forEach(dp -> assertEquals(expected[dp.get(id).get().toString().charAt(0) - 'A'], dp.get(resultMeasure).get(), 
					"For result n. " + (int)(dp.get(id).get().toString().charAt(0) - 'A')));
	}
}
