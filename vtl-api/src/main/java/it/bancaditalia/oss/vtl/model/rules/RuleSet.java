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
package it.bancaditalia.oss.vtl.model.rules;

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.SerIntPredicate;

public interface RuleSet extends Serializable
{
	public enum RuleSetType
	{
		VARIABLE, VALUE_DOMAIN;
	}
	
	public enum RuleType
	{
		EQ("=", c -> c == 0), 
		LT("<", c -> c < 0), 
		LE("<=", c -> c <= 0), 
		GT(">", c -> c > 0), 
		GE(">=", c -> c >= 0);
		
		private final String toString;
		private final SerIntPredicate test;
	
		RuleType(String toString, SerIntPredicate test)
		{
			this.toString = toString;
			this.test = test;
		}
		
		@Override
		public String toString()
		{
			return toString;
		}
		
		public boolean test(ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
		{
			int compare = left.compareTo(right);
			return test.test(compare);
		}
	}
	
	public RuleSetType getType();
}
