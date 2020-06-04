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
package it.bancaditalia.oss.vtl.session;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;
import java.util.ServiceLoader;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.environment.Workspace;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public interface VTLSession extends TransformationScheme
{
	public Engine getEngine();
	
	public Workspace getWorkspace();
	
	public VTLSession addStatements(String statements);

	public VTLSession addStatements(Reader reader) throws IOException;

	public VTLSession addStatements(InputStream inputStream, Charset charset) throws IOException;

	public VTLSession addStatements(Path path, Charset charset) throws IOException;

	public List<? extends VTLValueMetadata> compile();
	
	public static Iterable<VTLSession> getInstances()
	{
		return ServiceLoader.load(VTLSession.class);
	}
}