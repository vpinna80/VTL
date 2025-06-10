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
package it.bancaditalia.oss.vtl.impl.environment.spark.scalars;

import static it.bancaditalia.oss.vtl.impl.environment.spark.scalars.ScalarValueUDT.getTagAndUDTForClass;
import static java.util.Collections.nCopies;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mockStatic;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.List;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.UDTEncoder;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment;
import it.bancaditalia.oss.vtl.impl.types.data.BigDecimalValue;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.Frequency;
import it.bancaditalia.oss.vtl.impl.types.data.GenericTimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public class ScalarValueUDTTest
{
	private static SparkSession session;

	@BeforeAll
	public static void initializeSession()
	{
		System.setProperty("spark.sql.shuffle.partitions", "5");
		System.setProperty("spark.shuffle.compress", "false");
		System.setProperty("spark.shuffle.spill.compress", "false");
		System.setProperty("spark.broadcast.compress", "false");
		System.setProperty("spark.broadcast.compress", "false");
		System.setProperty("spark.kryo.registrationRequired", "true");
		System.setProperty("spark.kryoserializer.buffer", "1m");
		System.setProperty("spark.memory.fraction", "0.7");
		System.setProperty("spark.memory.storageFraction", "0.2");
		System.setProperty("spark.cleaner.periodicGC.interval", "30s");

		try (MockedStatic<ConfigurationManager> cmMock = mockStatic(ConfigurationManager.class, call -> {
				String method = call.getMethod().getName();
				switch (method)
				{
					case "getLocalPropertyValue": return call.getArgument(0, VTLProperty.class).getDefaultValue();
					case "getLocalPropertyValues": return List.of();
					default: return null;
				}
			}))
		{
			session = new SparkEnvironment().getSession();
		}
	}
	
	public static Stream<Arguments> testScalar()
	{
		return List.<ScalarValue<?, ?, ?, ?>>of(
				BigDecimalValue.of(new BigDecimal(1.0).divide(new BigDecimal(10))),
				BooleanValue.of(false),
				DateValue.of(LocalDate.now()),
				Frequency.M.get(),
				GenericTimeValue.of(DateValue.of(LocalDate.of(2020, 1, 1)), DateValue.of(LocalDate.of(2020, 12, 31))),
				IntegerValue.of(10L),
				StringValue.of("TEST"),
				TimePeriodValue.of(new MonthPeriodHolder(YearMonth.of(2020, 1)))
			).stream().map(elem -> Arguments.of(elem.getClass().getSimpleName(), elem));
	}
	
	@ParameterizedTest(name = "{0} - {1}")
	@MethodSource
	public <T extends ScalarValue<?, ?, ?, ?>> void testScalar(String name, T scalar)
	{
		UDTEncoder<?> agnostic = getTagAndUDTForClass(scalar.getClass()).getValue().getEncoder();
		@SuppressWarnings("unchecked")
		Encoder<T> encoder = (Encoder<T>) ExpressionEncoder.apply(agnostic);
		Dataset<T> dataset = session.createDataset(nCopies(1000, scalar), encoder);
		List<T> collected = dataset.collectAsList();
		assertEquals(1000, collected.size(), "size");
		for (int i = 0; i < 1000; i++)
			assertEquals(scalar, collected.get(i), "element");
	}
}
