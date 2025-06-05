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
package it.bancaditalia.oss.vtl.impl.transform.scope;

import static it.bancaditalia.oss.vtl.util.Utils.entryByKey;
import static java.util.stream.Collectors.toConcurrentMap;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class JoinApplyScope extends AbstractScope
{
	private static final long serialVersionUID = 1L;

	private final Map<VTLAlias, ScalarValue<?, ?, ?, ?>> joinValues;
	private final Map<VTLAlias, ScalarValueMetadata<?, ?>> joinMeta;
	private final TransformationScheme parent;

	public JoinApplyScope(TransformationScheme parent, VTLAlias measureName, DataPoint joinedDataPoint)
	{
		this.parent = parent;
		this.joinValues = joinedDataPoint.entrySet().stream()
				.filter(entryByKey(c -> {
					VTLAlias cAlias = c.getVariable().getAlias();
					return measureName.equals(cAlias.isComposed() ? cAlias.split().getValue() : cAlias);
				})).collect(toConcurrentMap(e -> {
					VTLAlias alias = e.getKey().getVariable().getAlias();
					return alias.isComposed() ? alias.split().getKey() : null;
				}, Entry::getValue));
		this.joinMeta = null;
	}

	public JoinApplyScope(TransformationScheme parent, VTLAlias measureName, Set<DataStructureComponent<?, ?, ?>> joinedComponents)
	{
		this.parent = parent;
		this.joinValues = null;
		this.joinMeta = joinedComponents.stream()
				.filter(c -> {
					VTLAlias cAlias = c.getVariable().getAlias();
					return measureName.equals(cAlias.isComposed() ? cAlias.split().getValue() : cAlias);
				})
				.collect(toConcurrentMap(c -> {
					VTLAlias alias = c.getVariable().getAlias();
					return alias.isComposed() ? alias.split().getKey() : null;
				}, DataStructureComponent::getVariable));
	}

	@Override
	public MetadataRepository getRepository()
	{
		return parent.getRepository();
	}

	@Override
	public VTLValue resolve(VTLAlias node)
	{
		if (joinValues != null && joinValues.containsKey(node))
			return joinValues.get(node);
		else
			return parent.resolve(node);
	}

	@Override
	public VTLValueMetadata getMetadata(VTLAlias node)
	{
		if (joinValues != null && joinValues.containsKey(node))
			return joinValues.get(node).getMetadata();
		else if (joinMeta != null && joinMeta.containsKey(node))
			return joinMeta.get(node);
		else
			return parent.getMetadata(node);
	}
	
	@Override
	public Optional<Statement> getRule(VTLAlias node)
	{
		return parent.getRule(node);
	}
}
