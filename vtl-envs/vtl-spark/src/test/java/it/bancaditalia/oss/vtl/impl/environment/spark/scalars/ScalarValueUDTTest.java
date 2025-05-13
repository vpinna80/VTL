package it.bancaditalia.oss.vtl.impl.environment.spark.scalars;

import static it.bancaditalia.oss.vtl.impl.environment.spark.scalars.ScalarValueUDT.getTagAndUDTForClass;
import static java.util.Collections.nCopies;
import static org.junit.jupiter.api.Assertions.assertEquals;

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

		session = new SparkEnvironment().getSession();
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
