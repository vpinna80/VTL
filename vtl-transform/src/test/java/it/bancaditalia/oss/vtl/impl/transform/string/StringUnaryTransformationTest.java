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

import static it.bancaditalia.oss.vtl.impl.environment.sampledata.SampleDataSets.SAMPLE13;
import static it.bancaditalia.oss.vtl.impl.transform.string.StringUnaryTransformation.StringOperator.LCASE;
import static it.bancaditalia.oss.vtl.impl.transform.string.StringUnaryTransformation.StringOperator.LTRIM;
import static it.bancaditalia.oss.vtl.impl.transform.string.StringUnaryTransformation.StringOperator.RTRIM;
import static it.bancaditalia.oss.vtl.impl.transform.string.StringUnaryTransformation.StringOperator.TRIM;
import static it.bancaditalia.oss.vtl.impl.transform.string.StringUnaryTransformation.StringOperator.UCASE;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.string.StringUnaryTransformation.StringOperator;
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerCollectors;

public class StringUnaryTransformationTest
{
	public static Stream<Arguments> test()
	{
		// "Filled String", "    Leading spaces", "Trailing spaces    ", "    Leading and trailing     ", "\"Quoted\" 'String'", "\t\b \n\r\f"

		return Stream.of(
				Arguments.of(TRIM,  new String[] {"Filled String", "Leading spaces",     "Trailing spaces",     "Leading and trailing",          "\"Quoted\" 'String'", "" } ),
				Arguments.of(LTRIM, new String[] {"Filled String", "Leading spaces",     "Trailing spaces    ", "Leading and trailing     ",     "\"Quoted\" 'String'", "\b \n\r\f"} ),
				Arguments.of(RTRIM, new String[] {"Filled String", "    Leading spaces", "Trailing spaces",     "    Leading and trailing",      "\"Quoted\" 'String'", "\t\b"} ),
				Arguments.of(UCASE, new String[] {"FILLED STRING", "    LEADING SPACES", "TRAILING SPACES    ", "    LEADING AND TRAILING     ", "\"QUOTED\" 'STRING'", "\t\b \n\r\f"} ),
				Arguments.of(LCASE, new String[] {"filled string", "    leading spaces", "trailing spaces    ", "    leading and trailing     ", "\"quoted\" 'string'", "\t\b \n\r\f"} )
			);
	}
	
	@ParameterizedTest(name = "{0}")
	@MethodSource
	public void test(StringOperator operator, String[] expected)
	{
		VarIDOperand left = new VarIDOperand(VTLAliasImpl.of("left"));
		Map<VTLAlias, DataSet> map = new HashMap<>();
		map.put(VTLAliasImpl.of("left"), SAMPLE13);
		TransformationScheme session = TestUtils.mockSession(map);

		StringUnaryTransformation sut = new StringUnaryTransformation(operator, left);
		DataSetStructure structure = (DataSetStructure) sut.getMetadata(session);
		
		DataSetComponent<Identifier, ?, ?> id = structure.getComponents(Identifier.class, STRINGDS).iterator().next();
		Optional<DataSetComponent<Measure, ?, ?>> measure = structure.getComponent(VTLAliasImpl.of("string_2"), Measure.class, STRINGDS);
		assertTrue(measure.isPresent(), "measure present in " + structure);
		
		try (Stream<DataPoint> stream = ((DataSet) sut.eval(session)).stream())
		{
			ConcurrentMap<String, String> resultMap = stream
				.map(dp -> new SimpleEntry<>((String) dp.get(id).get(), coalesce((String) dp.get(measure.get()).get(), "")))
				.collect(SerCollectors.entriesToMap());

			for (int i = 0; i < expected.length; i++)
			{
				String key = "" + (char) ('A' + i);
				byte[] bytes = resultMap.get(key).getBytes();
				assertArrayEquals(expected[i].getBytes(), bytes, operator.toString() + "[" + i +  "]");
			}
		}
	}}
