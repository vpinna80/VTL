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

import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE14;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE15;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireStringDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class ConcatTransformationTest
{
	private final static String RESULT[] = { "AK", "CC", "EG", null, "IA", "KE" };
	
	@Test
	public void test()
	{
		VarIDOperand left = new VarIDOperand("left"), right = new VarIDOperand("right");
		Map<String, DataSet> map = new HashMap<>();
		map.put("left", SAMPLE14);
		map.put("right", SAMPLE15);
		TransformationScheme session = TestUtils.mockSession(map);

		ConcatTransformation coTransformation = new ConcatTransformation(left, right);
		
		DataSetMetadata metadata = (DataSetMetadata) coTransformation.getMetadata(session);
		assertTrue(metadata.contains("string_2"));
		
		DataSet computedResult = (DataSet) coTransformation.eval(session);
		Optional<DataStructureComponent<Identifier, EntireStringDomainSubset, StringDomain>> oId = metadata.getComponent("string_1", Identifier.class, STRINGDS);		
		Optional<DataStructureComponent<Measure, EntireStringDomainSubset, StringDomain>> oMeasure = metadata.getComponent("string_2", Measure.class, STRINGDS);
		
		assertTrue(oId.isPresent(), "String id present");
		assertTrue(oMeasure.isPresent(), "String measure present");
		
		DataStructureComponent<Identifier, EntireStringDomainSubset, StringDomain> id = oId.get();
		DataStructureComponent<Measure, EntireStringDomainSubset, StringDomain> res = oMeasure.get();
		
		DataSet leftD = (DataSet) left.eval(session), 
				rightD = (DataSet) right.eval(session);
		
		computedResult.stream()
			.forEach(dp -> assertEquals(RESULT[dp.get(id).get().toString().charAt(0) - 'A'], dp.get(res).get(), 
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
