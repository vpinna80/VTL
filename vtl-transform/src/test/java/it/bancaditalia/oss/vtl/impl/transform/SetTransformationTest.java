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
package it.bancaditalia.oss.vtl.impl.transform;

import static it.bancaditalia.oss.vtl.impl.environment.samples.SampleDataSets.SAMPLE10;
import static it.bancaditalia.oss.vtl.impl.environment.samples.SampleDataSets.SAMPLE5;
import static it.bancaditalia.oss.vtl.impl.transform.ops.SetTransformation.SetOperator.INTERSECT;
import static it.bancaditalia.oss.vtl.impl.transform.ops.SetTransformation.SetOperator.SETDIFF;
import static it.bancaditalia.oss.vtl.impl.transform.ops.SetTransformation.SetOperator.SYMDIFF;
import static it.bancaditalia.oss.vtl.impl.transform.ops.SetTransformation.SetOperator.UNION;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.transform.ops.SetTransformation;
import it.bancaditalia.oss.vtl.impl.transform.ops.SetTransformation.SetOperator;
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class SetTransformationTest
{
	private TransformationScheme session;
	private VarIDOperand left, right;
	
	public static Stream<Arguments> twoDatasetTest()
	{
		return Stream.of(
				Arguments.of(INTERSECT, 3L),
				Arguments.of(SETDIFF, 3L), 
				Arguments.of(SYMDIFF, 6L), 
				Arguments.of(UNION, 9L)
			);
	}
	
	@BeforeEach
	public void before()
	{
		left = new VarIDOperand(VTLAliasImpl.of("left"));
		right = new VarIDOperand(VTLAliasImpl.of("right"));
		Map<VTLAlias, DataSet> map = new HashMap<>();
		map.put(VTLAliasImpl.of("left"), SAMPLE5);
		map.put(VTLAliasImpl.of("right"), SAMPLE10);
		session = TestUtils.mockSession(map);
	}
	
	@ParameterizedTest(name = "{0}")
	@MethodSource
	public void twoDatasetTest(SetOperator operator, long length)
	{
		SetTransformation setTransformation = new SetTransformation(operator, asList(left, right));
		
		DataSetStructure metadata = (DataSetStructure) setTransformation.getMetadata(session);
		assertTrue(metadata.contains(VTLAliasImpl.of("integer_1")));
		assertTrue(metadata.contains(VTLAliasImpl.of("string_1")));
		
		DataSet result = setTransformation.eval(session);
		
		assertEquals(length, result.size(), "Number of datapoints");
	}
}
