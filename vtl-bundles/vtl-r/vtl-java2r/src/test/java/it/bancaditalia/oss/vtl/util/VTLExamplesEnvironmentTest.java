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
package it.bancaditalia.oss.vtl.util;

import static it.bancaditalia.oss.vtl.util.VTLExamplesEnvironment.createSession;
import static it.bancaditalia.oss.vtl.util.VTLExamplesEnvironment.getCategories;
import static it.bancaditalia.oss.vtl.util.VTLExamplesEnvironment.getOperators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.session.VTLSession;

public class VTLExamplesEnvironmentTest
{
	public static Stream<Arguments> getOperatorsTest() throws IOException
	{
		List<Arguments> instances = new ArrayList<>();
		
		for (String category: getCategories())
			for (String operator: getOperators(category))
				instances.add(Arguments.of(category, operator));
		
		return instances.stream();
	}

	@ParameterizedTest(name = "{0} - {1}")
	@MethodSource
	public void getOperatorsTest(String category, String operator) throws IOException
	{
		VTLSession session = createSession(category, operator);
		session.compile().keySet().stream()
			.map(Statement::getAlias)
			.map(session::resolve)
			.filter(DataSet.class::isInstance)
			.map(DataSet.class::cast)
			.forEach(ds -> { 
				try(Stream<DataPoint> stream = ds.stream())
				{ 
					stream.forEach(System.out::println);
				}
			});
	}
}
