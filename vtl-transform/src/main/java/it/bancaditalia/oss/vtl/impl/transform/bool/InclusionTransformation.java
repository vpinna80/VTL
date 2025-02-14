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
package it.bancaditalia.oss.vtl.impl.transform.bool;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.util.SerBiPredicate.not;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.joining;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.EnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.SerBiPredicate;
import it.bancaditalia.oss.vtl.util.Utils;

public class InclusionTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> BOOL_VAR = BOOLEANDS.getDefaultVariable().as(Measure.class);

	public static enum InOperator implements SerBiPredicate<Set<? extends ScalarValue<?, ?, ?, ?>>, ScalarValue<?, ?, ?, ?>>
	{
		IN("in", Set::contains),
		NOTIN("not_in", not(Set::contains));

		private final String text;
		private final SerBiPredicate<Set<? extends ScalarValue<?, ?, ?, ?>>, ScalarValue<?, ?, ?, ?>> test;

		private InOperator(String text, SerBiPredicate<Set<? extends ScalarValue<?, ?, ?, ?>>, ScalarValue<?, ?, ?, ?>> test)
		{
			this.text = text;
			this.test = test;
		}

		@Override
		public boolean test(Set<? extends ScalarValue<?, ?, ?, ?>> t, ScalarValue<?, ?, ?, ?> u)
		{
			return test.test(t,  u);
		}
		
		@Override
		public String toString()
		{
			return text;
		}
	}

	private final InOperator operator;
	private final Set<ScalarValue<?, ?, ?, ?>> set;
	private final VTLAlias domainName;

	public InclusionTransformation(InOperator operator, Transformation operand, List<ScalarValue<?, ?, ?, ?>> list, VTLAlias domainName)
	{
		super(operand);
		this.operator = operator;
		this.set = new HashSet<>(coalesce(list, emptyList()));
		this.domainName = domainName;
	}

	@Override
	protected VTLValue evalOnScalar(MetadataRepository repo, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		checkSet(repo);
		return BooleanValue.of(operator.test(set, scalar));
	}

	@Override
	protected VTLValue evalOnDataset(MetadataRepository repo, DataSet dataset, VTLValueMetadata metadata)
	{
		DataStructureComponent<? extends Measure, ?, ?> measure = dataset.getMetadata().getMeasures().iterator().next();
		checkSet(repo);
		
		return dataset.mapKeepingKeys((DataSetMetadata) metadata, lineage -> LineageNode.of(this, lineage), 
				dp -> singletonMap(BOOL_VAR, BooleanValue.of(operator.test(set, dp.get(measure)))));
	}

	private void checkSet(MetadataRepository repo)
	{
		if (set.isEmpty())
		{
			EnumeratedDomainSubset<?, ?> domain = (EnumeratedDomainSubset<?, ?>) repo.getDomain(domainName)
					.orElseThrow(() -> new VTLUndefinedObjectException("Domain", domainName));
			set.addAll(domain.getCodeItems());
		}
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata value = operand.getMetadata(scheme);
		checkSet(scheme.getRepository());

		ScalarValue<?, ?, ?, ?> item1 = null;
		for (ScalarValue<?, ?, ?, ?> item: set)
			if (item1 == null)
				item1 = item;
			else
				if (!item1.getDomain().equals(item.getDomain()))
					throw new VTLIncompatibleTypesException(operator.toString().toLowerCase(), item1, item);
		
		if (item1 == null)
			throw new IllegalStateException("At least one item is expected for " + operator.toString().toLowerCase());
		
		if (value instanceof DataSetMetadata)
		{
			DataSetMetadata ds = (DataSetMetadata) value;

			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = ds.getMeasures();

			if (measures.size() != 1)
				throw new VTLSingletonComponentRequiredException(Measure.class, measures);
			
			DataStructureComponent<? extends Measure, ?, ?> measure = measures.iterator().next();
			if (!item1.getDomain().isAssignableFrom(measure.getVariable().getDomain()))
				if (measure.getVariable().getDomain().isAssignableFrom(item1.getDomain()))
				{
					// (try to) cast all items to the measure domain
					Set<? extends ScalarValue<?, ?, ?, ?>> set2 = Utils.getStream(set)
						.map(((ValueDomainSubset<?, ?>) measure.getVariable().getDomain())::cast)
						.collect(toSet());
					set.clear();
					set.addAll(set2);			
				}
				else
					throw new VTLIncompatibleTypesException(operator.toString().toLowerCase(), measure, item1.getDomain());
			
			
			return new DataStructureBuilder()
					.addComponents(ds.getIDs())
					.addComponent(BOOL_VAR)
					.build();
		}
		else
			return BOOLEAN;
	}

	@Override
	public String toString()
	{
		if (set.isEmpty())
			return operand + " " + operator + " " + domainName;
		else
			return operand + " " + operator + set.stream().map(Object::toString).collect(joining(", ", " {", "}"));
	}
}
