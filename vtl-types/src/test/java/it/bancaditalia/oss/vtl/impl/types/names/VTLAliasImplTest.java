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
package it.bancaditalia.oss.vtl.impl.types.names;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

import it.bancaditalia.oss.vtl.model.data.VTLAlias;

public class VTLAliasImplTest
{
	@Test
	public void testAliasMatches()
	{
		VTLAlias left = VTLAliasImpl.of(false, "ECB_CURRENCY");
		VTLAlias qLeft = VTLAliasImpl.of(true, "ECB_CURRENCY");
		VTLAlias qqLeft = VTLAliasImpl.of(true, "'ECB_CURRENCY'");
		VTLAlias right = VTLAliasImpl.of(false, "ecb_currency");
		VTLAlias qRight = VTLAliasImpl.of(true, "ecb_currency");
		VTLAlias qqRight = VTLAliasImpl.of(true, "'ecb_currency'");

		assertEquals(left.hashCode(), qLeft.hashCode(), "left == qLeft");
		assertEquals(left.hashCode(), qqLeft.hashCode(), "left == qqLeft");
		assertEquals(qLeft.hashCode(), qqLeft.hashCode(), "qLeft == qqLeft");

		assertEquals(right.hashCode(), qRight.hashCode(), "right == qRight");
		assertEquals(right.hashCode(), qqRight.hashCode(), "right == qqRight");
		assertEquals(qRight.hashCode(), qqRight.hashCode(), "qRight == qqRight");

		assertEquals(left, qLeft, "left == qLeft");
		assertEquals(left, qqLeft, "left == qqLeft");
		assertEquals(qLeft, qqLeft, "qLeft == qqLeft");
		assertEquals(qLeft, left, "qLeft == left");  
		assertEquals(qqLeft, left, "qqLeft == left");  
		assertEquals(qqLeft, qLeft, "qqLeft == qLeft");  

		assertEquals(right, qRight, "right == qRight");
		assertEquals(right, qqRight, "right == qqRight");
		assertEquals(qRight, qqRight, "qRight == qqRight");
		assertEquals(qRight, right, "qRight == right");  
		assertEquals(qqRight, right, "qqRight == right");  
		assertEquals(qqRight, qRight, "qqRight == qRight");  

		assertEquals(left.hashCode(), right.hashCode(), "left == right");
		assertEquals(left.hashCode(), qRight.hashCode(), "left == qRight");
		assertEquals(left.hashCode(), qqRight.hashCode(), "left == qqRight");
		assertEquals(qLeft.hashCode(), right.hashCode(), "qLeft == right");
		assertEquals(qLeft.hashCode(), qRight.hashCode(), "qLeft == qRight");
		assertEquals(qLeft.hashCode(), qqRight.hashCode(), "qLeft == qqRight");
		assertEquals(qqLeft.hashCode(), right.hashCode(), "qqLeft == right");
		assertEquals(qqLeft.hashCode(), qRight.hashCode(), "qqLeft == qRight");
		assertEquals(qqLeft.hashCode(), qqRight.hashCode(), "qqLeft == qqRight");

		assertEquals(left, right, "left == right");
		assertEquals(left, qRight, "left == qRight");
		assertEquals(left, qqRight, "left == qqRight");
		assertEquals(right, left, "right == left");
		assertEquals(right, qLeft, "right == qLeft");
		assertEquals(right, qqLeft, "right == qqLeft");
		assertEquals(qLeft, right, "qLeft == right");
		assertEquals(qRight, left, "qRight == left");
		assertEquals(qqLeft, right, "qqLeft == right");
		assertEquals(qqRight, left, "qqRight == left");

		assertNotEquals(qLeft, qRight, "qLeft != qRight");
		assertNotEquals(qLeft, qqRight, "qLeft != qqRight");
		assertNotEquals(qRight, qLeft, "qRight != qLeft");
		assertNotEquals(qRight, qqLeft, "qRight != qqLeft");
		assertNotEquals(qqLeft, qRight, "qqLeft != qRight");
		assertNotEquals(qqLeft, qqRight, "qqLeft != qqRight");
		assertNotEquals(qqRight, qLeft, "qqRight != qLeft");
		assertNotEquals(qqRight, qqLeft, "qqRight != qqLeft");
	}
}
