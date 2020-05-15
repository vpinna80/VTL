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
package it.bancaditalia.oss.vtl.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.session.VTLSessionHandler;

public class ScalarIT {
	
	private static Stream<Arguments> test() {
	    return Stream.of(
	      Arguments.of("+", 150L),
	      Arguments.of("-", 50L),
	      Arguments.of("*", 5000L),
	      Arguments.of("/", 2L)
	    );
	}
	
	@ParameterizedTest(name="Scalar operation {0} must equal {1}")
	@MethodSource
	public void test(String operator, long result) 
	{
		System.setProperty("NO_R", "true");
		String session = "Scalar operation " + operator;
		
		String script = "a:=100;\n"
					+ "b:=50;\n"
					+ "c:=a" + operator + "b;\n";
		VTLSessionHandler.addStatements(session, script);
		VTLSessionHandler.compile(session);
		Map<String, List<Object>> c = VTLSessionHandler.evalNode(session, "c");
		
		assertNotNull(c, "Null result");
		assertEquals(1, c.size(), "Empty result");
		assertNotNull(c.get("Scalar"), "Wrong Structure result");
		assertEquals(result, c.get("Scalar").get(0), "Wrong Value Result");
	}
}

