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
package it.bancaditalia.oss.vtl.model.data;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a component role
 * 
 * @author Valentino Pinna
 */
public interface Component
{
	public enum Role
	{
		COMPONENT(Component.class),
		IDENTIFIER(Identifier.class),
		NONIDENTIFIER(NonIdentifier.class),
		MEASURE(Measure.class),
		ATTRIBUTE(Attribute.class),
		VIRAL_ATTRIBUTE(ViralAttribute.class);
		
		private static final Map<Class<? extends Component>, Role> ENUM_TAGS = new HashMap<>();
		
		static
		{
			ENUM_TAGS.put(Component.class, COMPONENT);
			ENUM_TAGS.put(Identifier.class, IDENTIFIER);
			ENUM_TAGS.put(Measure.class, MEASURE);
			ENUM_TAGS.put(Attribute.class, ATTRIBUTE);
			ENUM_TAGS.put(ViralAttribute.class, VIRAL_ATTRIBUTE);
		}
		
		private final Class<? extends Component> clazz;

		Role(Class<? extends Component> clazz)
		{
			this.clazz = clazz;
		}

		public Class<? extends Component> roleClass()
		{
			return clazz;
		}
		
		public boolean matchesComponent(DataStructureComponent<?, ?, ?> component)
		{
			return clazz.isAssignableFrom(component.getRole());
		}
		
		public static Role from(Class<? extends Component> clazz)
		{
			return ENUM_TAGS.get(clazz);
		}
	}

	public interface Identifier extends Component {};

	public interface NonIdentifier extends Component {};

	public interface Attribute extends NonIdentifier {};

	public interface ViralAttribute extends Attribute {};

	public interface Measure extends NonIdentifier {};
}