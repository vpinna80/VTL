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
package it.bancaditalia.oss.vtl.impl.transform.clause;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.ops.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.scope.DatapointScope;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class CalcClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;

	public static class CalcClauseItem extends TransformationImpl
	{
		private static final long serialVersionUID = 1L;

		private final String name;
		private final Class<? extends ComponentRole> role;
		private final Transformation calcClause;

		public CalcClauseItem(Class<? extends ComponentRole> role, String name, Transformation calcClause)
		{
			this.name = name;
			this.calcClause = calcClause;
			this.role = role;
		}

		public String getName()
		{
			return name;
		}

		public Class<? extends ComponentRole> getRole()
		{
			return role;
		}

		@Override
		public String toString()
		{
			return (role != null ? role.getSimpleName().toLowerCase() + " " : "") + name + " := " + calcClause;
		}

		@Override
		public boolean isTerminal()
		{
			return calcClause.isTerminal();
		}

		@Override
		public Set<LeafTransformation> getTerminals()
		{
			return calcClause.getTerminals();
		}

		@Override
		public ScalarValue<?, ?, ?> eval(TransformationScheme session)
		{
			return (ScalarValue<?, ?, ?>) calcClause.eval(session);
		}

		@Override
		public VTLValueMetadata getMetadata(TransformationScheme scheme)
		{
			return calcClause.getMetadata(scheme);
		}

	}

	private final List<CalcClauseItem> items;
	private VTLDataSetMetadata metadata;

	public CalcClauseTransformation(List<CalcClauseItem> items)
	{
		this.items = items;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return items.stream().map(Transformation::getTerminals).flatMap(Set::stream).collect(toSet());
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		DataSet operand = (DataSet) getThisValue(session);

		return new LightFDataSet<>(metadata, ds -> ds.stream()
				.map(dp -> {
					DatapointScope dpSession = new DatapointScope(dp, metadata, session);
					// place original non-overriden components 
					DataPointBuilder builder = new DataPointBuilder(dp).delete(Utils.getStream(items)
									.map(CalcClauseItem::getName)
									.collect(toList())
							.toArray(new String[0]));
					
					// place calculated components 
					Utils.getStream(items)
						.forEach(item -> builder.add(metadata.getComponent(item.getName()).get(), item.eval(dpSession)));
					
					return builder.build(metadata);
				}), operand);
	}

	@Override
	public VTLDataSetMetadata getMetadata(TransformationScheme session)
	{
		if (metadata != null)
			return metadata;
		
		VTLValueMetadata operand = getThisMetadata(session);

		if (!(operand instanceof VTLDataSetMetadata))
			throw new VTLInvalidParameterException(operand, VTLDataSetMetadata.class);

		metadata = (VTLDataSetMetadata) operand;
		DataStructureBuilder builder = new DataStructureBuilder(metadata);

		for (CalcClauseItem item : items)
		{
			VTLValueMetadata itemMeta = item.getMetadata(session);
			ValueDomainSubset<?> domain;

			// get the domain of the calculated component
			if (itemMeta instanceof VTLDataSetMetadata)
			{
				// calc item expression is a component, check for mono-measure dataset
				VTLDataSetMetadata value = (VTLDataSetMetadata) itemMeta;
				if (value.getComponents(Measure.class).size() != 1)
					throw new VTLExpectedComponentException(Measure.class, value);

				domain = value.getComponents(Measure.class).iterator().next().getDomain();
			} 
			else
				domain = ((VTLScalarValueMetadata<?>) itemMeta).getDomain();

			Optional<DataStructureComponent<?, ?, ?>> maybePresent = metadata.getComponent(item.getName());
			
			if (maybePresent.isPresent())
			{
				// existing component
				DataStructureComponent<?, ?, ?> definedComponent = maybePresent.get();

				// disallow override of ids
				if (definedComponent.is(Identifier.class))
					throw new VTLInvariantIdentifiersException("calc", singleton(definedComponent.as(Identifier.class)));
				else if (!definedComponent.getDomain().isAssignableFrom(definedComponent.getDomain()))
					throw new VTLIncompatibleTypesException("Calc", definedComponent.getDomain(), definedComponent.getDomain());
				else if (item.getRole() != null && !definedComponent.is(item.getRole()))
				{
					// switch role (from a non-id to any)
					builder.removeComponent(definedComponent);
					DataStructureComponent<?, ?, ?> newComponent = new DataStructureComponentImpl<>(item.getName(), item.getRole(), definedComponent.getDomain());
					builder.addComponent(newComponent);
				}
			}
			else
			{
				// new component
				Class<? extends ComponentRole> newComponent = item.getRole() == null ? Measure.class : item.getRole();
				builder = builder.addComponent(new DataStructureComponentImpl<>(item.getName(), newComponent, domain));
			}
		}

		return metadata = builder.build();
	}

	@Override
	public String toString()
	{
		return items.stream().map(Object::toString).collect(joining(", ", "[calc ", "]"));
	}
}
