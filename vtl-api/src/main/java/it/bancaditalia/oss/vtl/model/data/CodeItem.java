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

/**
 * Representation of an item in an {@link EnumeratedDomainSubset}
 * 
 * @author Valentino Pinna
 *
 * @param <I> Implementation type
 * @param <S> The domain subset of the item
 * @param <D> The domain of the type
 */
public interface CodeItem<I extends CodeItem<I, R, S, D>, R extends Comparable<?> & Serializable, S extends EnumeratedDomainSubset<S, D>, D extends ValueDomain> extends ScalarValue<I, R, S, D>
{

}
