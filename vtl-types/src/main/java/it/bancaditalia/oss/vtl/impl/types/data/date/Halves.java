package it.bancaditalia.oss.vtl.impl.types.data.date;

import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.YEARS;
import static java.time.temporal.IsoFields.QUARTER_YEARS;
import static org.threeten.extra.TemporalFields.HALF_YEARS;

import java.io.Serializable;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.List;

public class OneHalf implements TemporalAmount, Serializable
{
	private static final long serialVersionUID = 1L;
	private static final OneHalf INSTANCE = new OneHalf();
	
	private OneHalf() {}
	
	public static OneHalf get()
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
		return List.of(HALF_YEARS);
	}

	@Override
	public long get(TemporalUnit unit)
	{
		if (unit == YEARS)
			return 0;
		if (unit == HALF_YEARS)
			return 1;
		if (unit == QUARTER_YEARS)
			return 2;
		if (unit == MONTHS)
			return 6;
		
		throw new UnsupportedTemporalTypeException("Unsupported unit " + unit);
	}

	@Override
	public Temporal addTo(Temporal temporal)
	{
		return temporal.plus(1, HALF_YEARS);
	}
}