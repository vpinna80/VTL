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
package it.bancaditalia.oss.vtl.impl.types.domain.tcds;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class TransformationCriterionScope implements TransformationScheme, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final VTLAlias domainAlias;
	private final ScalarValue<?, ?, ?, ?> value;
	private final ValueDomainSubset<?, ?> metadata;
	private final Map<Transformation, ?> holder = new ConcurrentHashMap<>();

	public TransformationCriterionScope(VTLAlias domainAlias, ScalarValue<?, ?, ?, ?> value)
	{
		this.domainAlias = domainAlias;
		this.value = value;
		this.metadata = value.getMetadata().getDomain();
	}

	public TransformationCriterionScope(VTLAlias domainAlias, ValueDomainSubset<?, ?> meta)
	{
		this.domainAlias = domainAlias;
		this.value = null;
		this.metadata = meta;
	}

	@Override
	public VTLValue resolve(VTLAlias alias)
	{
		if (domainAlias.equals(alias))
			return value;
		else
			throw new VTLUnboundAliasException(alias);
	}

	@Override
	public Optional<Statement> getRule(VTLAlias alias)
	{
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Map<Transformation, T> getResultHolder(Class<T> type)
	{
		if (VTLValueMetadata.class.isAssignableFrom(type))
			return (Map<Transformation, T>) holder;
		
		throw new UnsupportedOperationException(type.getName());
	}

	@Override
	public MetadataRepository getRepository()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public ScalarValueMetadata<?, ?> getMetadata(VTLAlias alias)
	{
		if (domainAlias.equals(alias))
			return ScalarValueMetadata.of(metadata);
		else
			throw new VTLUnboundAliasException(alias);
	}

	@Override
	public HierarchicalRuleSet findHierarchicalRuleset(VTLAlias alias)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public DataPointRuleSet findDatapointRuleset(VTLAlias alias)
	{
		throw new UnsupportedOperationException();
	}
}