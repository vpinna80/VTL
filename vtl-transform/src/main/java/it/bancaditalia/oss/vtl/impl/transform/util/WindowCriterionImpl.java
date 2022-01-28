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
package it.bancaditalia.oss.vtl.impl.transform.util;

import static it.bancaditalia.oss.vtl.impl.transform.util.LimitClause.CURRENT_DATA_POINT;
import static it.bancaditalia.oss.vtl.impl.transform.util.LimitClause.UNBOUNDED_FOLLOWING;
import static it.bancaditalia.oss.vtl.impl.transform.util.LimitClause.UNBOUNDED_PRECEDING;
import static it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion.LimitType.DATAPOINTS;
import static it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion.LimitType.RANGE;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion;

public class WindowCriterionImpl implements WindowCriterion, Serializable
{
	public static final WindowCriterion DATAPOINTS_UNBOUNDED_PRECEDING_TO_CURRENT = 
			new WindowCriterionImpl(DATAPOINTS, UNBOUNDED_PRECEDING, CURRENT_DATA_POINT);
	public static final WindowCriterion DATAPOINTS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING = 
			new WindowCriterionImpl(DATAPOINTS, UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING);
	
	private static final long serialVersionUID = 1L;

	private final LimitType type;
	private final LimitCriterion infBound;
	private final LimitCriterion supBound;

	public WindowCriterionImpl(LimitType type, LimitCriterion infBound, LimitCriterion supBound)
	{
		this.type = type;
		this.infBound = infBound;
		this.supBound = supBound;
	}

	@Override
	public LimitCriterion getInfBound()
	{
		return infBound;
	}

	@Override
	public LimitCriterion getSupBound()
	{
		return supBound;
	}
	
	@Override
	public LimitType getType()
	{
		return type;
	}
	
	@Override
	public String toString()
	{
		return (type == RANGE ? "range" : "data points") + " between " + infBound + " and " + supBound;
	}
}