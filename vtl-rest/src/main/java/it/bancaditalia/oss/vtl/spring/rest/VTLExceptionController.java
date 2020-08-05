package it.bancaditalia.oss.vtl.spring.rest;

import static org.springframework.http.HttpStatus.BAD_REQUEST;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import it.bancaditalia.oss.vtl.exceptions.VTLUnboundNameException;
import it.bancaditalia.oss.vtl.spring.rest.exception.VTLInvalidSessionException;

@ControllerAdvice
public class VTLExceptionController
{
	@ExceptionHandler(value = ParseCancellationException.class)
	public ResponseEntity<Object> exception(ParseCancellationException e)
	{
		return new ResponseEntity<>("Syntax error: " + e.getLocalizedMessage(), BAD_REQUEST);
	}

	@ExceptionHandler(value = VTLUnboundNameException.class)
	public ResponseEntity<Object> exception(VTLUnboundNameException e)
	{
		return new ResponseEntity<>(e.getLocalizedMessage(), BAD_REQUEST);
	}

	@ExceptionHandler(value = VTLInvalidSessionException.class)
	public ResponseEntity<Object> exception(VTLInvalidSessionException e)
	{
		return new ResponseEntity<>(e.getLocalizedMessage(), BAD_REQUEST);
	}
}
