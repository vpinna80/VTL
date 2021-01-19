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
package it.bancaditalia.oss.vtl.impl.transform.testutils;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;

import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;

public enum SampleVariables
{
	MEASURE_INTEGER_1, MEASURE_INTEGER_2, MEASURE_INTEGER_3, 
	MEASURE_NUMBER_1, MEASURE_NUMBER_2, MEASURE_NUMBER_3, MEASURE_NUMBER_4,
	MEASURE_STRING_1, MEASURE_STRING_2, MEASURE_STRING_3, MEASURE_STRING_4, MEASURE_STRING_5, MEASURE_STRING_6, MEASURE_STRING_7, 
	MEASURE_BOOLEAN_1, MEASURE_BOOLEAN_2, MEASURE_BOOLEAN_3, MEASURE_BOOLEAN_4, 
	MEASURE_DATE_1, MEASURE_DATE_2, MEASURE_DATE_3, 
	IDENT_INTEGER_1, IDENT_INTEGER_2, IDENT_INTEGER_3, 
	IDENT_NUMBER_1, IDENT_NUMBER_2, IDENT_NUMBER_3, IDENT_NUMBER_4, 
	IDENT_STRING_1, IDENT_STRING_2, IDENT_STRING_3, IDENT_STRING_4, IDENT_STRING_5, IDENT_STRING_6, IDENT_STRING_7, 
	IDENT_BOOLEAN_1, IDENT_BOOLEAN_2, IDENT_BOOLEAN_3, IDENT_BOOLEAN_4, 
	IDENT_DATE_1, IDENT_DATE_2, IDENT_DATE_3, 
	ATTRIB_INTEGER_1, ATTRIB_INTEGER_2, ATTRIB_INTEGER_3, 
	ATTRIB_NUMBER_1, ATTRIB_NUMBER_2, ATTRIB_NUMBER_3, ATTRIB_NUMBER_4, 
	ATTRIB_STRING_1, ATTRIB_STRING_2, ATTRIB_STRING_3, ATTRIB_STRING_4, ATTRIB_STRING_5, ATTRIB_STRING_6, ATTRIB_STRING_7,
	ATTRIB_BOOLEAN_1, ATTRIB_BOOLEAN_2, ATTRIB_BOOLEAN_3, ATTRIB_BOOLEAN_4,
	ATTRIB_DATE_1, ATTRIB_DATE_2, ATTRIB_DATE_3; 

	private DataStructureComponentImpl<?, ?, ?> component;
	
	private SampleVariables()
	{
		String elem[] = name().split("_");
		Class<? extends Component> role = null;
		switch (elem[0])
		{
			case "MEASURE": role = Measure.class; break;
			case "ATTRIB": role = Attribute.class; break;
			case "IDENT": role = Identifier.class; break;
		}
		
		ValueDomainSubset<?> domain = null;
		switch (elem[1])
		{
			case "NUMBER": domain = NUMBERDS; break;
			case "INTEGER": domain = INTEGERDS; break;
			case "STRING": domain = STRINGDS; break;
			case "BOOLEAN": domain = BOOLEANDS; break;
			case "DATE": domain = DATEDS; break;
		}

		component = new DataStructureComponentImpl<>(elem[1] + "_" + elem[2], role, domain);
	}
	
	public DataStructureComponent<?, ?, ?> getComponent(int level)
	{
		return component.rename(name -> name.split("_")[0] + "_" + level);
	}
	
	public String getType()
	{
		return toString().split("_")[1];
	}
	
	public int getIndex()
	{
		return Integer.parseInt(toString().split("_")[2]);
	}
}
