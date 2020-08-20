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
package it.bancaditalia.oss.vtl.engine;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * The main parsing engine.
 * Classes implementing this interface provide a way to translate VTL source code into
 * VTL operations that a Transformation Scheme can be applied to.  
 *  
 * @author Valentino Pinna
 */
public interface Engine
{
	/**
	 * Parse a string containing VTL Statements.
	 * @param statements A {@link String} containing valid VTL syntax.
	 * @return A stream of statements.
	 */
	public Stream<Statement> parseRules(String statements);

	/**
	 * Parse VTL Statements that will be read from the given reader.
	 * The {@link Reader} will be consumed entirely and closed.
	 * 
	 * @param reader A {@link Reader} containing valid VTL syntax. 
	 * @return A stream of statements.
	 */
	public Stream<Statement> parseRules(Reader reader) throws IOException;

	/**
	 * Parse VTL Statements that will be read from the given input stream.
	 * The {@link InputStream} will be consumed entirely and closed.  
	 * 
	 * @param inputStream An {@link InputStream} containing valid VTL syntax. 
	 * @param charset The {@link Charset} to be used to translate characters from the stream.
	 * @return A stream of statements.
	 */
	public Stream<Statement> parseRules(InputStream inputStream, Charset charset) throws IOException;

	/**
	 * Parse VTL Statements that will be read from a file pointed by given {@link Path}.  
	 * 
	 * @param path A {@link Path} describing a local file containing valid VTL syntax.
	 * @param charset The {@link Charset} to be used to translate characters from the file contents.
	 * @return A stream of statements.
	 */
	public Stream<Statement> parseRules(Path path, Charset charset) throws IOException;
}