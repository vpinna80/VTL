package it.bancaditalia.oss.vtl.impl.types.data;

import it.bancaditalia.oss.vtl.impl.types.domain.DurationDomains;

public interface TimeHolder
{
	public TimePeriodValue wrap(DurationDomains frequency);

	public String getPeriodIndicator();
}
