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
package it.bancaditalia.oss.vtl.util;

import java.io.Serializable;
import java.util.Arrays;

public class GenericTuple implements Serializable, Cloneable
{
	private static final long serialVersionUID = 1L;
	
	private final Serializable[] values;

	public GenericTuple(Serializable[] values)
	{
		this.values = values;
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(values);
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (!(obj instanceof GenericTuple))
			return false;
		GenericTuple other = (GenericTuple) obj;
		if (!Arrays.equals(values, other.values))
			return false;
		return true;
	}

	public int size()
	{
		return values.length;
	}
	
	public Serializable get1() { return values[0]; }
	public Serializable get2() { return values[1]; }
	public Serializable get3() { return values[2]; }
	public Serializable get4() { return values[3]; }
	public Serializable get5() { return values[4]; }
	public Serializable get6() { return values[5]; }
	public Serializable get7() { return values[6]; }
	public Serializable get8() { return values[7]; }
	public Serializable get9() { return values[8]; }
	public Serializable get10() { return values[9]; }
	public Serializable get11() { return values[10]; }
	public Serializable get12() { return values[11]; }
	public Serializable get13() { return values[12]; }
	public Serializable get14() { return values[13]; }
	public Serializable get15() { return values[14]; }
	public Serializable get16() { return values[15]; }
	public Serializable get17() { return values[16]; }
	public Serializable get18() { return values[17]; }
	public Serializable get19() { return values[18]; }
	public Serializable get20() { return values[19]; }
	
	public void set1(Serializable value) { values[0] = value; }
	public void set2(Serializable value) { values[1] = value; }
	public void set3(Serializable value) { values[2] = value; }
	public void set4(Serializable value) { values[3] = value; }
	public void set5(Serializable value) { values[4] = value; }
	public void set6(Serializable value) { values[5] = value; }
	public void set7(Serializable value) { values[6] = value; }
	public void set8(Serializable value) { values[7] = value; }
	public void set9(Serializable value) { values[8] = value; }
	public void set10(Serializable value) { values[9] = value; }
	public void set11(Serializable value) { values[10] = value; }
	public void set12(Serializable value) { values[11] = value; }
	public void set13(Serializable value) { values[12] = value; }
	public void set14(Serializable value) { values[13] = value; }
	public void set15(Serializable value) { values[14] = value; }
	public void set16(Serializable value) { values[15] = value; }
	public void set17(Serializable value) { values[16] = value; }
	public void set18(Serializable value) { values[17] = value; }
	public void set19(Serializable value) { values[18] = value; }
	public void set20(Serializable value) { values[19] = value; }
}
