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

import java.io.Serializable;

import it.bancaditalia.oss.vtl.model.domain.EnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

/**
 * Representation of an item in an {@link EnumeratedDomainSubset}
 * 
 * @author Valentino Pinna
 *
 * @param <C> Implementation type
 * @param <R> The representation value type
 * @param <S> The {@link ValueDomainSubset} of the item
 * @param <D> The parent {@link ValueDomain} of the domain of the item
 */
public interface CodeItem<C extends CodeItem<C, R, S, D>, R extends Comparable<?> & Serializable, S extends EnumeratedDomainSubset<S, C, R, D>, D extends ValueDomain> extends ScalarValue<C, R, S, D>
{

}
