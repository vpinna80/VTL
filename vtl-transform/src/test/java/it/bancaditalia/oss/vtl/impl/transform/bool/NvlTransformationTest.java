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
package it.bancaditalia.oss.vtl.impl.transform.bool;

import static it.bancaditalia.oss.vtl.impl.data.samples.SampleDataSets.SAMPLE5;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static java.lang.Long.MAX_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import it.bancaditalia.oss.vtl.impl.transform.ConstantOperand;
import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireIntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class NvlTransformationTest
{
	@Test
	public void test()
	{
		VarIDOperand left = new VarIDOperand("left");
		ConstantOperand right = new ConstantOperand(IntegerValue.of(MAX_VALUE));
		Map<String, DataSet> map = new HashMap<>();
		map.put("left", SAMPLE5);
		TransformationScheme session = TestUtils.mockSession(map);

		NvlTransformation isnt = new NvlTransformation(left, right);
		DataSetMetadata structure = (DataSetMetadata) isnt.getMetadata(session);
		
		Optional<DataStructureComponent<Measure, EntireIntegerDomainSubset, IntegerDomain>> component = structure.getComponent("integer_1", Measure.class, INTEGERDS);
		assertTrue(component.isPresent(), "integer_var measure present");
		
		long nullCount = ((DataSet) isnt.eval(session)).stream()
			.filter(dp -> right.eval(null).equals(dp.get(component.get())))
			.count();
		
		assertEquals(1, nullCount, "1 null value replaced");
	}
}
