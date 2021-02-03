package it.bancaditalia.oss.vtl.impl.transform;

import static it.bancaditalia.oss.vtl.impl.transform.ops.SetTransformation.SetOperator.INTERSECT;
import static it.bancaditalia.oss.vtl.impl.transform.ops.SetTransformation.SetOperator.SETDIFF;
import static it.bancaditalia.oss.vtl.impl.transform.ops.SetTransformation.SetOperator.SYMDIFF;
import static it.bancaditalia.oss.vtl.impl.transform.ops.SetTransformation.SetOperator.UNION;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE5;
import static it.bancaditalia.oss.vtl.impl.transform.testutils.SampleDataSets.SAMPLE10;
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
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
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
		left = new VarIDOperand("left");
		right = new VarIDOperand("right");
		Map<String, DataSet> map = new HashMap<>();
		map.put("left", SAMPLE5);
		map.put("right", SAMPLE10);
		session = TestUtils.mockSession(map);
	}
	
	@ParameterizedTest(name = "{0}")
	@MethodSource
	public void twoDatasetTest(SetOperator operator, long length)
	{
		SetTransformation setTransformation = new SetTransformation(operator, asList(left, right));
		
		DataSetMetadata metadata = (DataSetMetadata) setTransformation.getMetadata(session);
		assertTrue(metadata.contains("INTEGER_1"));
		assertTrue(metadata.contains("STRING_1"));
		
		DataSet result = setTransformation.eval(session);
		
		assertEquals(length, result.size(), "Number of datapoints");
	}
}
