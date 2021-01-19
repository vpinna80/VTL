package it.bancaditalia.oss.vtl.impl.transform.time;

import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE7;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE8;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE9;
import static it.bancaditalia.oss.vtl.impl.transform.time.FillTimeSeriesTransformation.FillMode.ALL;
import static it.bancaditalia.oss.vtl.impl.transform.time.FillTimeSeriesTransformation.FillMode.SINGLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.testutils.TestUtils;
import it.bancaditalia.oss.vtl.impl.transform.time.FillTimeSeriesTransformation.FillMode;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class FillTimeSeriesTransformationTest
{
	private TransformationScheme session;
	
	public static Stream<Arguments> test()
	{
		// 7: 2020-01-01 2020-01-02 2020-01-03 2020-01-04 2020-01-05 2020-01-06
		// 8: 2020-01-01            2020-01-03            2020-01-05            2020-01-07            2020-01-09            2020-01-11 
		// 9:                       2020-01-03 2020-01-04 2020-01-05            2020-01-07 2020-01-08            2020-01-10

		return Stream.of(
				Arguments.of("SAMPLE7",                 TestUtils.concat(SAMPLE7),                   SINGLE,  6),
				Arguments.of("SAMPLE8",                 TestUtils.concat(SAMPLE8),                   SINGLE, 11),
				Arguments.of("SAMPLE9",                 TestUtils.concat(SAMPLE9),                   SINGLE,  8),
				Arguments.of("SAMPLE7 SAMPLE8",         TestUtils.concat(SAMPLE7, SAMPLE8),          SINGLE, 17),
				Arguments.of("SAMPLE7 SAMPLE8",         TestUtils.concat(SAMPLE7, SAMPLE8),          ALL,    22),
				Arguments.of("SAMPLE8 SAMPLE9",         TestUtils.concat(SAMPLE8, SAMPLE9),          SINGLE, 19),
				Arguments.of("SAMPLE8 SAMPLE9",         TestUtils.concat(SAMPLE8, SAMPLE9),          ALL,    22),
				Arguments.of("SAMPLE7 SAMPLE8 SAMPLE9", TestUtils.concat(SAMPLE7, SAMPLE8, SAMPLE9), ALL,    33)
			);
	}
	
	@ParameterizedTest(name = "{0} {2}")
	@MethodSource
	public void test(String name, DataSet sample, FillMode mode, int expectedSize)
	{
		Map<String, DataSet> map = new HashMap<>();
		map.put("operand", sample);
		session = TestUtils.mockSession(map);

		FillTimeSeriesTransformation ftsTransformation = new FillTimeSeriesTransformation(new VarIDOperand("operand"), mode);
		
		DataSet computedResult = (DataSet) ftsTransformation.eval(session);
		if (expectedSize != computedResult.size())
			System.out.println(computedResult.toString());
		assertEquals(expectedSize, computedResult.size(), "Dataset size");
	}
}
