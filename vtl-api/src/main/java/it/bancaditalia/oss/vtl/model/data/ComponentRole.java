/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.model.data;

public interface ComponentRole 
{
	public enum Roles
	{
		IDENTIFIER(ComponentRole.Identifier.class), 
		MEASURE(ComponentRole.Measure.class), 
		ATTRIBUTE(ComponentRole.Attribute.class), 
		VIRAL_ATTRIBUTE(ComponentRole.ViralAttribute.class);
		
		private final Class<? extends ComponentRole> clazz;

		Roles(Class<? extends ComponentRole> clazz)
		{
			this.clazz = clazz;
		}

		public Class<? extends ComponentRole> getClazz()
		{
			return clazz;
		}
	}

	public interface Identifier extends ComponentRole {};

	public interface NonIdentifier extends ComponentRole {};

	public interface Attribute extends ComponentRole.NonIdentifier {};

	public interface ViralAttribute extends ComponentRole.Attribute {};

	public interface Measure extends ComponentRole.NonIdentifier {};
}