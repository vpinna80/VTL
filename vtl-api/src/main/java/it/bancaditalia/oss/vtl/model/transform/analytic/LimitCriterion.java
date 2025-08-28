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
package it.bancaditalia.oss.vtl.model.transform.analytic;

/**
 * The criterion defining the bounds of each of the windows in a VTL analytic invocation.
 * 
 * @author Valentino Pinna
 *
 */
public interface LimitCriterion
{
	public static enum LimitDirection
	{
		PRECEDING, FOLLOWING
	}

	/**
	 * @return The direction of the bound from current data point
	 */
	public LimitDirection getDirection();

	/**
	 * @return The distance of the bound from current data point
	 */
	public int getCount();

	/**
	 * Tests if this {@link LimitCriterion} is of the form UNBOUNDED PRECEDING or UNBOUNDED FOLLOWING
	 * 
	 * @return true if the coondition holds
	 */
	public boolean isUnbounded();
}
