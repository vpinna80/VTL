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

import static it.bancaditalia.oss.vtl.impl.types.data.date.VTLChronoUnit.SEMESTERS;
import static java.time.temporal.ChronoUnit.YEARS;
import static java.time.temporal.IsoFields.QUARTER_YEARS;

import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.time.temporal.ValueRange;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

public enum VTLChronoField implements TemporalField
{
	SEMESTER_OF_YEAR("SemesterOfYear", SEMESTERS, YEARS, ValueRange.of(1, 2),
			new Class[] { SemesterPeriodHolder.class, QuarterPeriodHolder.class, MonthPeriodHolder.class }),
	QUARTER_OF_SEMESTER("QuarterOfSemester", QUARTER_YEARS, SEMESTERS, ValueRange.of(1, 2),
			new Class[] { QuarterPeriodHolder.class, MonthPeriodHolder.class });

	private final String name;
	private final TemporalUnit baseUnit;
	private final TemporalUnit rangeUnit;
	private final ValueRange range;
	private List<Class<? extends TemporalAccessor>> supporting;

	@SuppressWarnings("unchecked")
	private VTLChronoField(String name, TemporalUnit base, TemporalUnit range, ValueRange allowed, Class<?>[] classes)
	{
		this.name = name;
		this.baseUnit = base;
		this.rangeUnit = range;
		this.range = allowed;
		this.supporting = Arrays.stream(classes).map(c -> (Class<? extends TemporalAccessor>) c).collect(Collectors.toList());
	}

	@Override
	public String getDisplayName(Locale locale)
	{
		return name;
	}

	@Override
	public TemporalUnit getBaseUnit()
	{
		return baseUnit;
	}

	@Override
	public TemporalUnit getRangeUnit()
	{
		return rangeUnit;
	}

	@Override
	public ValueRange range()
	{
		return range;
	}

	@Override
	public boolean isDateBased()
	{
		return true;
	}

	@Override
	public boolean isTimeBased()
	{
		return false;
	}

	@Override
	public boolean isSupportedBy(TemporalAccessor temporal)
	{
		return supporting.stream().filter(c -> c.isInstance(temporal)).findAny().isPresent();
	}

	@Override
	public ValueRange rangeRefinedBy(TemporalAccessor temporal)
	{
		return range;
	}

	@Override
	public long getFrom(TemporalAccessor temporal)
	{
		Optional<Class<? extends TemporalAccessor>> findAny = supporting.stream()
			.filter(c -> c.isInstance(temporal))
			.findAny();
		
		if (!findAny.isPresent())
			throw new UnsupportedTemporalTypeException(temporal.getClass().toString() + " - " + this);
		
		return temporal.getLong(this);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Temporal> R adjustInto(R temporal, long newValue)
	{
		return (R) temporal.with(this, newValue);
	}

	@Override
	public String toString()
	{
		return name;
	}
}