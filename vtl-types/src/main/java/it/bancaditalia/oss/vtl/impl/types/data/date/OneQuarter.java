package it.bancaditalia.oss.vtl.impl.types.data.date;

import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.YEARS;
import static java.time.temporal.IsoFields.QUARTER_YEARS;
import static org.threeten.extra.TemporalFields.HALF_YEARS;

import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.List;

public class OneQuarter implements TemporalAmount
{
	private static final OneQuarter INSTANCE = new OneQuarter();
	
	private OneQuarter() {}
	
	public static OneQuarter get()
	{
		return INSTANCE;
	}
	@Override
	public Temporal subtractFrom(Temporal temporal)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public List<TemporalUnit> getUnits()
	{
		return List.of(QUARTER_YEARS);
	}

	@Override
	public long get(TemporalUnit unit)
	{
		if (unit == YEARS)
			return 0;
		if (unit == HALF_YEARS)
			return 0;
		if (unit == QUARTER_YEARS)
			return 1;
		if (unit == MONTHS)
			return 3;
		
		throw new UnsupportedTemporalTypeException("Unsupported unit " + unit);
	}

	@Override
	public Temporal addTo(Temporal temporal)
	{
		return temporal.plus(1, QUARTER_YEARS);
	}
}