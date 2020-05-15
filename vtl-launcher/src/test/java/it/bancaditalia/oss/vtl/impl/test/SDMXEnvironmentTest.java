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
package it.bancaditalia.oss.vtl.impl.test;

import static java.time.temporal.IsoFields.QUARTER_OF_YEAR;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.DateTimeException;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;

import it.bancaditalia.oss.sdmx.exceptions.SdmxException;
import it.bancaditalia.oss.vtl.impl.session.VTLSessionHandler;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.session.VTLSession;

public class SDMXEnvironmentTest
{
	public static void main(String[] args) throws SdmxException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, FileNotFoundException, IOException
	{
//		testParse();SDMXEnvironment
		System.setProperty("NO_R", "true");
		System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "4");
		VTLSession test = VTLSessionHandler.getSession("test");
//		System.setProperty("vtl.sdmx.keep.identifiers", "true");
		test
				.addStatements(new FileReader("./target/test-classes/snippet.vtl"))
//				.addStatements("a := 'sdmx:ECB.EXR.A.USD.EUR.SP00.A';") 
//				.addStatements("b := +a;") 
//				.addStatements("tmp2 := inner_join('sdmx:ECB.EXR.D..EUR.SP00.A'[rename OBS_VALUE to M1]#M1 as t1, 'sdmx:ECB.EXR.D..EUR.SP00.A'[rename OBS_VALUE to M2]#M2 as t2);")
//				.addStatements("tmp3 := tmp2 + 2;")
//				.addStatements("tmp4 := inner_join(tmp3, 'sdmx:ECB.EXR.D..EUR.SP00.A'[rename OBS_VALUE to M3]#M3 as t3);")
//				.addStatements("define operator MONTHP(time_value measure) is" +
//												"    cast(cast(time_value,string,\"MM\"),integer)" +
//												"end operator;")
//				.addStatements("compOfBasket := 'sdmx:ECB.ICP.M.U2.N.010000.4.ANR'[calc M := MONTHP(TIME_PERIOD)];")
//				.addStatements("compOfBasket := 'sdmx:ECB.ICP.M.U2.N.010000.4.ANR'[keep OBS_VALUE][calc identifier YEAR := cast(time_agg(\"A\",TIME_PERIOD), time)];")
//				.addStatements("compWeight := 'sdmx:ECB.ICP.A.U2.N.010000.4.INW'[rename TIME_PERIOD to YEAR][keep OBS_VALUE][rename OBS_VALUE to WEIGHT];")
//				.addStatements("expected := 'sdmx:ECB.ICP.M.U2.N.010000.3.CTG'[keep OBS_VALUE];")
//				.addStatements("monthlyWeight := inner_join(compWeight, compOfBasket)[calc MONTHLY_WEIGHT := OBS_VALUE * WEIGHT / 1000][keep MONTHLY_WEIGHT][rename MONTHLY_WEIGHT to OBS_VALUE];")
//				.addStatements("calculated := compOfBasket * monthlyWeight;")
				.compile();
		long now = System.currentTimeMillis();
		String res = test.resolve("errors", VTLValue.class).toString();
		System.out.println("Time: " + (System.currentTimeMillis() - now) + "ms");
		System.out.println(res);
//		try (Paginator pager = new Paginator(VTLSessionHandler.getSession("test").resolve("ds_filter", DataSet.class)))
//		{
//			Map<String, List<Object>> res;
//			while (!(res = pager.more(20)).values().iterator().next().isEmpty())
//				System.out.println(res);
//		}
	}

	public static void testParse()
	{
		List<DateTimeFormatter>    FORMATTERS  = new ArrayList<>();
		
		FORMATTERS.add(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"));
		FORMATTERS.add(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm"));
		FORMATTERS.add(DateTimeFormatter.ofPattern("yyyy-MM-dd hh"));
		FORMATTERS.add(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		FORMATTERS.add(DateTimeFormatter.ofPattern("yyyy-MM"));
		FORMATTERS.add(DateTimeFormatter.ofPattern("yyyy"));
		FORMATTERS.add(new DateTimeFormatterBuilder()
				.appendPattern("yyyy-'Q'q")
				.parseCaseInsensitive()
		        .toFormatter());

		for (DateTimeFormatter formatter: FORMATTERS)
			try
			{
				System.out.println(formatter.parse("2020-03").isSupported(QUARTER_OF_YEAR));
			}
			catch (DateTimeException e)
			{
				System.out.println(e.getMessage());
			}
		
		System.exit(0);
	}
}