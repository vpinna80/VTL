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
package it.bancaditalia.oss.vtl.model.transform;

import java.io.Serializable;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;

/**
 * The representation of a VTL transformation.
 * @author Valentino Pinna
 *
 */
public interface Transformation extends Serializable
{
	/**
	 * @return True if this {@link Transformation} do not require previous computations before returning a result.
	 */
	public boolean isTerminal();
	
	/**
	 * A set of all the {@link LeafTransformation}s recursively referenced by this {@link Transformation}.
	 * 
	 * The set is empty if {@link #isTerminal()} is true.
	 * 
	 * @return the set
	 */
	public Set<LeafTransformation> getTerminals();
	
	/**
	 * Evaluates this transformation in the context of the given {@link TransformationScheme}.
	 * 
	 * @param scheme the transformation scheme
	 * @return the result of the computation of this {@link Transformation}.
	 */
	public VTLValue eval(TransformationScheme scheme);
	
	/**
	 * Checks and returns the metadata of this {@link Transformation} within the given {@link TransformationScheme}.
	 * 
	 * @param scheme the transformation scheme
	 * @return the metadata of this {@link Transformation}.
	 * @throws VTLException if the metadata is incoherent within the given {@link TransformationScheme}.
	 */
	public VTLValueMetadata getMetadata(TransformationScheme scheme);
}
