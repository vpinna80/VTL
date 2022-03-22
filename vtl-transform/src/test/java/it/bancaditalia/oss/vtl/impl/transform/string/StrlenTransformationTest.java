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

import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE13;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireIntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerCollectors;

public class StrlenTransformationTest
{
	public static Stream<Arguments> test()
	{
		return Stream.of(Arguments.of("Strlen 1", SAMPLE13, new String[] {"A", "B", "C", "D", "E", "F"}, new long[] { 13, 18, 19, 29, 17, 6 } ));
	}
	
	@ParameterizedTest(name = "{0}")
	@MethodSource
	public void test(String testName, DataSet operand, String[] resultKeys, long[] resultValues)
	{
		VarIDOperand left = new VarIDOperand("left");
		Map<String, DataSet> map = new HashMap<>();
		map.put("left", operand);
		TransformationScheme session = TestUtils.mockSession(map);

		StrlenTransformation st = new StrlenTransformation(left);
		DataSetMetadata structure = (DataSetMetadata) st.getMetadata(session);
		
		DataStructureComponent<Identifier, ? extends StringDomainSubset<?>, StringDomain> id = structure.getComponents(Identifier.class, STRINGDS).iterator().next();
		Optional<DataStructureComponent<Measure, EntireIntegerDomainSubset, IntegerDomain>> measure = structure.getComponent("integer_var", Measure.class, INTEGERDS);
		assertTrue(measure.isPresent(), "integer_var result");
		
		DataSet ds = (DataSet) st.eval(session);

		ConcurrentMap<?, ?> resultMap = ds.stream()
			.map(dp -> new SimpleEntry<>(((StringValue<?, ?>) dp.get(id)).get(), ((IntegerValue<?, ?>) dp.get(measure.get())).get()))
			.collect(SerCollectors.entriesToMap());
		
		for (int i = 0; i < resultKeys.length; i++)
			assertEquals(resultValues[i], resultMap.get(resultKeys[i]), "String length");
	}}
