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
package org.sdmx.vtl;

import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

public class SyntaxError
{
    private final Recognizer<?, ?> recognizer;
    private final Object offendingSymbol;
    private final int line;
    private final int charPositionInLine;
    private final String message;
    private final RecognitionException e;

    SyntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e)
    {
        this.recognizer = recognizer;
        this.offendingSymbol = offendingSymbol;
        this.line = line;
        this.charPositionInLine = charPositionInLine;
        this.message = msg;
        this.e = e;
    }

    public Recognizer<?, ?> getRecognizer()
    {
        return recognizer;
    }

    public Object getOffendingSymbol()
    {
        return offendingSymbol;
    }

    public int getLine()
    {
        return line;
    }

    public int getCharPositionInLine()
    {
        return charPositionInLine;
    }

    public String getMessage()
    {
        return message;
    }

    public RecognitionException getException()
    {
        return e;
    }
    
    @Override
    public String toString()
    {
    	return "Syntax error in line " + line;
    }
}