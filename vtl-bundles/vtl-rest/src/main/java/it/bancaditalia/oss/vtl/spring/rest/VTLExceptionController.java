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
package it.bancaditalia.oss.vtl.spring.rest;

import static org.springframework.http.HttpStatus.BAD_REQUEST;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import it.bancaditalia.oss.vtl.exceptions.VTLUnboundAliasException;
import it.bancaditalia.oss.vtl.spring.rest.exception.VTLInvalidSessionException;

@ControllerAdvice
public class VTLExceptionController
{
	@ExceptionHandler(value = ParseCancellationException.class)
	public ResponseEntity<Object> exception(ParseCancellationException e)
	{
		return new ResponseEntity<>("Syntax error: " + e.getLocalizedMessage(), BAD_REQUEST);
	}

	@ExceptionHandler(value = VTLUnboundAliasException.class)
	public ResponseEntity<Object> exception(VTLUnboundAliasException e)
	{
		return new ResponseEntity<>(e.getLocalizedMessage(), BAD_REQUEST);
	}

	@ExceptionHandler(value = VTLInvalidSessionException.class)
	public ResponseEntity<Object> exception(VTLInvalidSessionException e)
	{
		return new ResponseEntity<>(e.getLocalizedMessage(), BAD_REQUEST);
	}
}
