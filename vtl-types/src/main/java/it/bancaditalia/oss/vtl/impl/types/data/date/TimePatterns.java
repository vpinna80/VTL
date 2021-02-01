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
package it.bancaditalia.oss.vtl.impl.types.data.date;

import static it.bancaditalia.oss.vtl.impl.types.data.date.VTLChronoField.SEMESTER_OF_YEAR;
import static java.time.format.SignStyle.NOT_NEGATIVE;
import static java.time.format.TextStyle.FULL;
import static java.time.format.TextStyle.NARROW;
import static java.time.format.TextStyle.SHORT;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.DAY_OF_WEEK;
import static java.time.temporal.ChronoField.DAY_OF_YEAR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static java.time.temporal.IsoFields.QUARTER_OF_YEAR;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;

public class TimePatterns
{
	private static final Map<Pattern, UnaryOperator<DateTimeFormatterBuilder>> PATTERNS = new LinkedHashMap<>();
	private static final Pattern DATE_LITERAL_ELEMENT = Pattern.compile("^([-/ ]|\\\\.)(.*)$");
	private static final Map<String, DateTimeFormatter> FORMATTERS_CACHE = new ConcurrentHashMap<>();  
	
	static {
		PATTERNS.put(Pattern.compile("^(YYYY)(.*)$"), dtf -> dtf.appendValue(YEAR, 4));
		PATTERNS.put(Pattern.compile("^(YYY)(.*)$"), dtf -> dtf.appendValue(YEAR, 3));
		PATTERNS.put(Pattern.compile("^(YY)(.*)$"), dtf -> dtf.appendValue(YEAR, 2));
		PATTERNS.put(Pattern.compile("^(S)(.*)$"), dtf -> dtf.appendValue(SEMESTER_OF_YEAR, 1));
		PATTERNS.put(Pattern.compile("^(Q)(.*)$"), dtf -> dtf.appendValue(QUARTER_OF_YEAR, 1));
		PATTERNS.put(Pattern.compile("^(M[Oo][Nn][Tt][Hh]3)(.*)$"), dtf -> dtf.appendText(MONTH_OF_YEAR, SHORT));
		PATTERNS.put(Pattern.compile("^(M[Oo][Nn][Tt][Hh]1)(.*)$"), dtf -> dtf.appendText(MONTH_OF_YEAR, NARROW));
		PATTERNS.put(Pattern.compile("^(M[Oo][Nn][Tt][Hh])(.*)$"), dtf -> dtf.appendText(MONTH_OF_YEAR, FULL));
		PATTERNS.put(Pattern.compile("^(D[Aa][Yy]3)(.*)$"), dtf -> dtf.appendText(DAY_OF_WEEK, SHORT));
		PATTERNS.put(Pattern.compile("^(D[Aa][Yy]1)(.*)$"), dtf -> dtf.appendText(DAY_OF_WEEK, NARROW));
		PATTERNS.put(Pattern.compile("^(D[Aa][Yy])(.*)$"), dtf -> dtf.appendText(DAY_OF_WEEK, FULL));
		PATTERNS.put(Pattern.compile("^(MM)(.*)$"), dtf -> dtf.appendValue(MONTH_OF_YEAR, 2));
		PATTERNS.put(Pattern.compile("^(M)(.*)$"), dtf -> dtf.appendValue(MONTH_OF_YEAR, 1, 2, NOT_NEGATIVE));
		PATTERNS.put(Pattern.compile("^(PPP)(.*)$"), dtf -> dtf.appendValue(DAY_OF_YEAR, 3));
		PATTERNS.put(Pattern.compile("^(DD)(.*)$"), dtf -> dtf.appendValue(DAY_OF_MONTH, 2));
		PATTERNS.put(Pattern.compile("^(D)(.*)$"), dtf -> dtf.appendValue(DAY_OF_MONTH, 1, 2, NOT_NEGATIVE));
	}

	public static <T extends TimeValue<?, ?, ?>> TemporalAccessor parseString(String string, String mask)
	{
		return getFormatter(mask).parse(string);
	}

	public static String parseTemporal(TemporalAccessor temporal, String mask)
	{
		return getFormatter(mask).format(temporal);
	}

	private static DateTimeFormatter getFormatter(final String mask)
	{
		DateTimeFormatter formatter = getFormatter(mask);
		
		if (formatter == null)
		{
			String remainingMask = mask;
			
			// Transform the VTL date mask into a DateTimeFormatter
			DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
			while (!remainingMask.isEmpty())
			{
				boolean found = false;
				for (Pattern pattern: PATTERNS.keySet())
					if (!found)
					{
						Matcher matcher = pattern.matcher(remainingMask);
						if (matcher.find())
						{
							builder = PATTERNS.get(pattern).apply(builder);
							remainingMask = matcher.group(2);
							found = true;
						}
					}
				
				if (!found)
				{
					Matcher matcher = DATE_LITERAL_ELEMENT.matcher(remainingMask);
					if (matcher.find())
					{
						builder = builder.appendLiteral(matcher.group(1).replaceAll("\\\\", ""));
						remainingMask = matcher.group(2);
					}
					else
						throw new IllegalStateException("Unrecognized mask in csv header: " + remainingMask);
				}
			}
	
			formatter = builder.toFormatter();
			FORMATTERS_CACHE.put(mask, formatter);
		}

		return formatter;
	}
}
