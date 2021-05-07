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
package it.bancaditalia.oss.vtl.engine;

import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

/**
 * Interface that represents a single VTL rule.
 * 
 * @author Valentino Pinna
 *
 */
public interface Statement
{
	/**
	 * @return The id of the statement, like the rule name at the left side of an assignment statement. 
	 */
	public String getId();

	/**
	 * Computes the metadata of the rule in the context of a Transformation Scheme. 
	 * 
	 * @param scheme The transformation scheme
	 * @return The metadata for this Statement
	 */
	public VTLValueMetadata getMetadata(TransformationScheme scheme);

	/**
	 * Computes this statement in the context of the provided scheme and returns the result.
	 * 
	 * @param scheme the context scheme
	 * @return the result of the computation.
	 */
	public VTLValue eval(TransformationScheme scheme);

	/**
	 * @return a list of primitive nodes, such as aliases or constants, on which this statement depends on
	 */
	public Set<LeafTransformation> getTerminals();

	/**
	 * @return true if it makes sense to cache the result of evaluating this statement.
	 */
	public boolean isCacheable();
	
	/**
	 * If available returns the lineage for this statement
	 * 
	 * @return the lineage if any
	 */
	public Lineage getLineage();
}