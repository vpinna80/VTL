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
package it.bancaditalia.oss.vtl.session;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;

import it.bancaditalia.oss.vtl.engine.Engine;
import it.bancaditalia.oss.vtl.environment.Workspace;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

/**
 * This interface provide the user with the ability to manage all VTL code, data and metadata 
 * by providing high-level methods that the user can interact with.
 * 
 * @author Valentino Pinna
 *
 */
public interface VTLSession extends TransformationScheme, Serializable
{
	/**
	 * @return the computing {@link Engine} used by this session
	 */
	public Engine getEngine();
	
	/**
	 * @return the {@link Workspace} used by this session
	 */
	public Workspace getWorkspace();

	/**
	 * Adds some VTL statements to this session. Code is parsed but not compiled.
	 *  
	 * @param statements A string containing the statements to add
	 * @return {@code this} instance.
	 */
	public VTLSession addStatements(String statements);

	/**
	 * Adds some VTL statements to this session. Code is parsed but not compiled.
	 * The {@link Reader} will be consumed entirely and closed.
	 * 
	 * @param reader a {@link Reader} which provides the VTL statements to add.
	 * @return {@code this} instance.
	 */
	public VTLSession addStatements(Reader reader) throws IOException;

	/**
	 * Adds some VTL statements to this session. Code is parsed but not compiled.
	 * The {@link InputStream} will be consumed entirely and closed.
	 *  
	 * @param inputStream an {@link InputStream} which provides the VTL statements to add
	 * @param charset a {@link Charset} instance which will be used to interpret the stream contents.
	 * @return {@code this} instance.
	 */
	public VTLSession addStatements(InputStream inputStream, Charset charset) throws IOException;

	/**
	 * Adds some VTL statements to this session. Code is parsed but not compiled.
	 *  
	 * @param path a {@link Path} describing the position of a local file which provides the VTL statements to add
	 * @param charset a {@link Charset} instance which will be used to interpret the file contents.
	 * @return {@code this} instance.
	 */
	public VTLSession addStatements(Path path, Charset charset) throws IOException;

	/**
	 * Compile all the VTL code submitted to this session, and returns the metadata of
	 * all the objects contained in this session.
	 *  
	 * @return a {@link List} of {@link VTLValueMetadata} instances, describing a VTL object each.
	 */
	public List<VTLValueMetadata> compile();
}