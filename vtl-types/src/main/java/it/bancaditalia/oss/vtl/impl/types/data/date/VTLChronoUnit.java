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

import java.time.Duration;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;

public enum VTLChronoUnit implements TemporalUnit
{
	SEMESTERS("Semesters", Duration.ofSeconds(31556952L / 2));
//	QUARTERS("Semesters", Duration.ofSeconds(31556952L / 4));

	private final String name;
	private final Duration duration;

	private VTLChronoUnit(String name, Duration estimatedDuration)
	{
		this.name = name;
		this.duration = estimatedDuration;
	}

	@Override
	public Duration getDuration()
	{
		return duration;
	}

	@Override
	public boolean isDurationEstimated()
	{
		return false;
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
	public boolean isSupportedBy(Temporal temporal)
	{
		return temporal.isSupported(this);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Temporal> R addTo(R temporal, long amount)
	{
		return (R) temporal.plus(amount, this);
	}

	@Override
	public long between(Temporal temporal1Inclusive, Temporal temporal2Exclusive)
	{
		return temporal1Inclusive.until(temporal2Exclusive, this);
	}

	@Override
	public String toString()
	{
		return name;
	}
}