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
package it.bancaditalia.oss.vtl.impl.transform.aggregation;

import static it.bancaditalia.oss.vtl.impl.transform.aggregation.OffsetTransformation.OffsetDirection.LAG;
import static it.bancaditalia.oss.vtl.impl.types.operators.AnalyticOperator.FIRST_VALUE;
import static it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion.LimitDirection.FOLLOWING;
import static it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion.LimitDirection.PRECEDING;
import static it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion.LimitType.DATAPOINTS;
import static java.util.stream.Collectors.joining;

import java.util.List;

import it.bancaditalia.oss.vtl.impl.transform.util.LimitClause;
import it.bancaditalia.oss.vtl.impl.transform.util.WindowCriterionImpl;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class OffsetTransformation extends SimpleAnalyticTransformation
{
	private static final long serialVersionUID = 1L;

	public enum OffsetDirection
	{
		LAG, LEAD
	}
	
	private final ScalarValue<?, ?, ?, ?> defaultValue;

	public OffsetTransformation(OffsetDirection direction, Transformation operand, IntegerValue<?> offset, ScalarValue<?, ?, ?, ?> defaultValue, List<String> partitionBy, List<OrderByItem> orderByClause)
	{
		super(FIRST_VALUE, operand, partitionBy, orderByClause, 
				new WindowCriterionImpl(DATAPOINTS, 
						new LimitClause(direction == LAG ? PRECEDING : FOLLOWING, Math.abs(offset.get())),
						new LimitClause(direction == LAG ? PRECEDING : FOLLOWING, Math.abs(offset.get()))));

		this.defaultValue = defaultValue;
	}

	@Override
	public String toString()
	{
		String operator = windowCriterion.getInfBound().getDirection() == FOLLOWING ? "lead" : "lag";
		long offset = Math.abs(windowCriterion.getInfBound().getCount());
		return operator + "(" + operand + ", " + offset + ", " + defaultValue + " over (" 
				+ (partitionBy == null || partitionBy.isEmpty() ? "" : partitionBy.stream().collect(joining(", ", " partition by ", " ")))
				+ (orderByClause == null || orderByClause.isEmpty() ? "" : orderByClause.stream().map(Object::toString).collect(joining(", ", " order by ", " ")))
				+ ")";
	}

}
