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
"use strict";

function suggest(ed, opts, offset)
{
    function MyListener()
    {
        this.reportAmbiguity = function(recognizer, dfa, startIndex, stopIndex, exact, ambigAlts, configs) { /*debugger*/ }
        this.reportAttemptingFullContext = function(recognizer, dfa, startIndex, stopIndex, conflictingAlts, configs) { /*debugger*/ }
        this.reportContextSensitivity = function(recognizer, dfa, startIndex, stopIndex, prediction, configs) { /*debugger*/ }
        this.syntaxError = (recognizer, offendingSymbol, line, column, msg, e) => {
            var ctx = e ? e.ctx : recognizer._ctx;
            var state = e ? e.offendingState : recognizer.state;
            result.push({
                list: looklit(range(recognizer.getExpectedTokens(ctx, state))),
                from: cm.findPosH(cm.getCursor(), - (offset + (offendingSymbol.stop - offendingSymbol.start + 1)), 'char'),
                to: vtl.editor.editorImplementation.getCursor()
            });
        }
    };

    var cm = vtl.editor.editorImplementation;
    var cursorPos = cm.getCursor();
    var startLine = cursorPos.line;
    var curRange = cm.findWordAt(cm.getCursor());
    if (curRange.head.ch > cursorPos.ch)
        curRange.head.ch = cursorPos.ch;
    var word = cm.getDoc().getRange(curRange.anchor, curRange.head).trim();
    var symbols = vtl.editor.parser.parser.parserWrapper.parser.literalNames.filter(t => t != null).map(t => t.substring(1, t.length - 1));
    offset = offset || (symbols.indexOf(word) >= 0 ? 0 : word.length);

    // determine start location of statement
    var idx = -1;
    while (startLine > 0 && (idx < 0 || startLine === cursorPos.line && idx > cursorPos.ch)) {
        var lineText = cm.getLine(startLine);
        if (startLine === cursorPos.line)
            lineText = lineText.substring(0, cursorPos.ch);
        idx = cm.getLine(lineText).indexOf(';');
        startLine--;
    }

    var text = cm.getDoc().getRange({ line: startLine, ch: idx + 1 }, cursorPos);
    text = text.substring(0, text.length - offset).trim();
    var parser = new vtl.gen.VtlParser(new vtl.antlr.CommonTokenStream(new vtl.gen.VtlLexer(new vtl.antlr.InputStream(text))));
    parser._interp.predictionMode = vtl.antlr.atn.PredictionMode.LL_EXACT_AMBIG_DETECTION;
    var outer = this;
    var result = [];

    //parser._errHandler = new vtl.antlr.error.BailErrorStrategy/*IgnoreErrorStrategy*/();
    parser.removeErrorListeners(); 
    parser.addErrorListener(new MyListener());
    try {
        // invoke the parser and catch the parsing exception
        var statement = parser.statement();
    } catch (e) {
        if (!(e instanceof vtl.antlr.error.ParseCancellationException))
            throw e;
        else if (result.length === 0) {
            result = [{
                list: looklit(range(e.cause.recognizer.atn.getExpectedTokens(e.cause.offendingState, e.cause.ctx))),
                from: cm.findPosH(cm.getCursor(), - (offset + (e.cause.offendingToken.stop - e.cause.offendingToken.start + 1)), 'char'),
                to: vtl.editor.editorImplementation.getCursor()
            }];
        }
    }

    if (result.length !== 0) {
        result = result.reduce((a, b) => a.from.ch <= b.from.ch ? a : b);

	var identifiers = vtl.editor.getAllTokens().map(t => t.token).filter(t => looksym(t.type) === 'IDENTIFIER').map(t => t.text);

        if (result.list.length === 1 && result.list[0] === 'IDENTIFIER')
            result.list = identifiers;
        else if (result.list.indexOf('IDENTIFIER') >= 0)
            result.list = result.list.filter(t => t !== 'IDENTIFIER').concat(identifiers);
    }

    if (result.from.ch < result.to.ch)
        result.list = result.list.filter(t => t.match(new RegExp("^" + cm.getDoc().getRange(result.from, result.to), "i")));

    result.list = result.list.filter((value, index, self) => self.indexOf(value) === index);
    return result.list.length > 0 ? result : null;
}

function range(start, end)
{
    if (typeof start == 'number')
        if (typeof end == 'number' && end > start)
            return new Array(end - start).fill().map((_, idx) => start + idx)
        else
            return [start];
    else if (start instanceof Array)
        return start.map(range).flat();
    else
        return start.intervals.map(i => range(i.start, i.stop)).flat();
}

function looksym(sym)
{
    var symbolicNames = vtl.editor.parser.parser.parserWrapper.parser.symbolicNames;
    if (typeof sym == 'number')
        return sym == vtl.antlr.Token.EOF ? "##EOF##" : symbolicNames[sym];
    else
        return sym.map(looksym);
}

function looklit(sym)
{
    var symbolicNames = vtl.editor.parser.parser.parserWrapper.parser.symbolicNames;
    var literalNames = vtl.editor.parser.parser.parserWrapper.parser.literalNames;
    if (typeof sym == 'number')
        return sym == vtl.antlr.Token.EOF ? "##EOF##" : literalNames[sym]
                ? literalNames[sym].substring(1, literalNames[sym].length - 1)
                : symbolicNames[sym];
    else
        return sym.map(looklit);
}

function lookrule(rule)
{
    var ruleNames = vtl.editor.parser.parser.parserWrapper.parser.ruleNames;
    if (typeof rule == 'number')
        return rule == vtl.antlr.Token.EOF ? "##EOF##" : ruleNames[rule];
    else if (rule instanceof vtl.antlr.atn.ATNState)
        return lookrule(rule.ruleIndex);
    else
        return rule.map(lookrule);
}

(function() {
    /*
    cm-meta cm-number cm-keyword cm-atom cm-def cm-variable cm-variable-2 cm-variable-3 cm-type 
    cm-property cm-operator cm-comment cm-string cm-string-2 cm-qualifier cm-error cm-attribute 
    cm-tag cm-link 
    */

    //
    // Rule Styles
    //
    var ruleStyles = {};
    //ruleStyles['start'] = 'cm-keyword';
    //ruleStyles['statement'] = 'cm-keyword';
    //ruleStyles['optionalExpr'] = 'cm-keyword';
    //ruleStyles['expr'] = 'cm-keyword';
    //ruleStyles['bracketClause'] = 'cm-keyword';
    //ruleStyles['exprComplex'] = 'cm-keyword';
    //ruleStyles['timeExpr'] = 'cm-keyword';
    //ruleStyles['defHierarchical'] = 'cm-keyword';
    //ruleStyles['hierRuleSignature'] = 'cm-keyword';
    //ruleStyles['conditioningItem'] = 'cm-keyword';
    //ruleStyles['ruleClauseHierarchical'] = 'cm-keyword';
    //ruleStyles['ruleItemHierarchical'] = 'cm-keyword';
    //ruleStyles['codeItemRelation'] = 'cm-keyword';
    //ruleStyles['codeItemRelationClause'] = 'cm-keyword';
    //ruleStyles['defDatapoint'] = 'cm-keyword';
    //ruleStyles['ruleClauseDatapoint'] = 'cm-keyword';
    //ruleStyles['ruleItemDatapoint'] = 'cm-keyword';
    //ruleStyles['rulesetSignature'] = 'cm-keyword';
    //ruleStyles['varSignature'] = 'cm-keyword';
    //ruleStyles['defOperator'] = 'cm-keyword';
    //ruleStyles['parameterItem'] = 'cm-keyword';
    //ruleStyles['callFunction'] = 'cm-keyword';
    //ruleStyles['exprAtom'] = 'cm-keyword';
    //ruleStyles['unaryFunctionName'] = 'cm-keyword';
    //ruleStyles['unaryStrToIntFunctionName'] = 'cm-keyword';
    //ruleStyles['unaryNumberFunctionName'] = 'cm-keyword';
    //ruleStyles['unaryStringFunctionName'] = 'cm-keyword';
    //ruleStyles['binaryFunctionName'] = 'cm-keyword';
    //ruleStyles['ref'] = 'cm-keyword';
    //ruleStyles['identifierList'] = 'cm-keyword';
    //ruleStyles['lists'] = 'cm-keyword';
    //ruleStyles['evalExpr'] = 'cm-keyword';
    //ruleStyles['castExpr'] = 'cm-keyword';
    //ruleStyles['periodExpr'] = 'cm-keyword';
    //ruleStyles['timeShiftExpr'] = 'cm-keyword';
    //ruleStyles['timeSeriesExpr'] = 'cm-keyword';
    //ruleStyles['timeAggExpr'] = 'cm-keyword';
    //ruleStyles['validationExpr'] = 'cm-keyword';
    //ruleStyles['validationDatapoint'] = 'cm-keyword';
    //ruleStyles['validationHierarchical'] = 'cm-keyword';
    //ruleStyles['erCode'] = 'cm-keyword';
    //ruleStyles['erLevel'] = 'cm-keyword';
    //ruleStyles['hierarchyExpr'] = 'cm-keyword';
    //ruleStyles['datasetClause'] = 'cm-keyword';
    //ruleStyles['anFunctionClause'] = 'cm-keyword';
    //ruleStyles['partitionByClause'] = 'cm-keyword';
    //ruleStyles['orderByClause'] = 'cm-keyword';
    //ruleStyles['windowingClause'] = 'cm-keyword';
    //ruleStyles['limitClauseItem'] = 'cm-keyword';
    //ruleStyles['joinExpr'] = 'cm-keyword';
    //ruleStyles['joinClause'] = 'cm-keyword';
    //ruleStyles['joinOperand'] = 'cm-keyword';
    //ruleStyles['joinBody'] = 'cm-keyword';
    //ruleStyles['joinCalcClause'] = 'cm-keyword';
    //ruleStyles['joinCalcClauseItem'] = 'cm-keyword';
    //ruleStyles['joinCalcExpr'] = 'cm-keyword';
    //ruleStyles['joinAggClause'] = 'cm-keyword';
    //ruleStyles['joinAggClauseItem'] = 'cm-keyword';
    //ruleStyles['joinAggExpr'] = 'cm-keyword';
    //ruleStyles['joinKeepClause'] = 'cm-keyword';
    //ruleStyles['joinDropClause'] = 'cm-keyword';
    //ruleStyles['joinFilterClause'] = 'cm-keyword';
    //ruleStyles['joinRenameClause'] = 'cm-keyword';
    //ruleStyles['joinApplyClause'] = 'cm-keyword';
    //ruleStyles['anFunction'] = 'cm-keyword';
    //ruleStyles['aggrFunctionClause'] = 'cm-keyword';
    //ruleStyles['getFiltersClause'] = 'cm-keyword';
    //ruleStyles['getFilterClause'] = 'cm-keyword';
    //ruleStyles['aggrClause'] = 'cm-keyword';
    //ruleStyles['filterClause'] = 'cm-keyword';
    //ruleStyles['renameClause'] = 'cm-keyword';
    //ruleStyles['aggrFunction'] = 'cm-keyword';
    //ruleStyles['calcClause'] = 'cm-keyword';
    //ruleStyles['calcClauseItem'] = 'cm-keyword';
    //ruleStyles['calcExpr'] = 'cm-keyword';
    //ruleStyles['dropClause'] = 'cm-keyword';
    //ruleStyles['dropClauseItem'] = 'cm-keyword';
    //ruleStyles['keepClause'] = 'cm-keyword';
    //ruleStyles['keepClauseItem'] = 'cm-keyword';
    //ruleStyles['unpivotExpr'] = 'cm-keyword';
    //ruleStyles['pivotExpr'] = 'cm-keyword';
    //ruleStyles['subspaceExpr'] = 'cm-keyword';
    //ruleStyles['subspaceClause'] = 'cm-keyword';
    //ruleStyles['inBetweenClause'] = 'cm-keyword';
    //ruleStyles['setExpr'] = 'cm-keyword';
    //ruleStyles['subscriptExpr'] = 'cm-keyword';
    //ruleStyles['aggrInvocation'] = 'cm-keyword';
    //ruleStyles['aggrInvocationMember'] = 'cm-keyword';
    //ruleStyles['aggrFunctionName'] = 'cm-keyword';
    //ruleStyles['groupingClause'] = 'cm-keyword';
    //ruleStyles['havingClause'] = 'cm-keyword';
    //ruleStyles['returnAll'] = 'cm-keyword';
    //ruleStyles['componentRole'] = 'cm-keyword';
    //ruleStyles['viralAttribute'] = 'cm-keyword';
    //ruleStyles['logBase'] = 'cm-keyword';
    //ruleStyles['exponent'] = 'cm-keyword';
    //ruleStyles['persistentDatasetID'] = 'cm-keyword';
    //ruleStyles['datasetID'] = 'cm-keyword';
    //ruleStyles['rulesetID'] = 'cm-keyword';
    //ruleStyles['varID'] = 'cm-keyword';
    //ruleStyles['componentID'] = 'cm-keyword';
    //ruleStyles['operatorID'] = 'cm-keyword';
    //ruleStyles['routineName'] = 'cm-keyword';
    //ruleStyles['joinKeyword'] = 'cm-keyword';
    //ruleStyles['constant'] = 'cm-keyword';
    //ruleStyles['componentType2'] = 'cm-keyword';
    //ruleStyles['scalarType'] = 'cm-keyword';
    //ruleStyles['basicScalarType'] = 'cm-keyword';
    //ruleStyles['valueDomainName'] = 'cm-keyword';
    //ruleStyles['setName'] = 'cm-keyword';
    //ruleStyles['scalarTypeConstraint'] = 'cm-keyword';
    //ruleStyles['dataType'] = 'cm-keyword';
    //ruleStyles['componentType'] = 'cm-keyword';
    //ruleStyles['datasetType'] = 'cm-keyword';
    //ruleStyles['compConstraint'] = 'cm-keyword';
    //ruleStyles['multModifier'] = 'cm-keyword';
    //ruleStyles['rulesetType'] = 'cm-keyword';
    //ruleStyles['dpRuleset'] = 'cm-keyword';
    //ruleStyles['hrRuleset'] = 'cm-keyword';
    //ruleStyles['prodValueDomains'] = 'cm-keyword';
    //ruleStyles['prodVariables'] = 'cm-keyword';
    //ruleStyles['operatorType'] = 'cm-keyword';
    //ruleStyles['inputParameterType'] = 'cm-keyword';
    //ruleStyles['outputParameterType'] = 'cm-keyword';
    //ruleStyles['scalarSetType'] = 'cm-keyword';
    //ruleStyles['retainType'] = 'cm-keyword';
    //ruleStyles['defineDatapointRuleset'] = 'cm-keyword';
    //ruleStyles['endDatapointRuleset'] = 'cm-keyword';
    //ruleStyles['endHierarchicalRuleset'] = 'cm-keyword';
    //ruleStyles['defineDataStructure'] = 'cm-keyword';
    vtl.editor.setDefaultRuleStyles(ruleStyles);

    var tokenStyles = {};

    //
    // Token Styles
    //
    tokenStyles['COLON'] = 'cm-keyword';
    tokenStyles['SEMICOLON'] = 'cm-keyword';
    tokenStyles['OBRACKET'] = 'cm-bracket';
    tokenStyles['CBRACKET'] = 'cm-bracket';
    tokenStyles['OBRACE'] = 'cm-bracket';
    tokenStyles['CBRACE'] = 'cm-bracket';
    tokenStyles['ASSIGN'] = 'cm-keyword';
    tokenStyles['PLUS'] = 'cm-operator';
    tokenStyles['DASH'] = 'cm-operator';
    tokenStyles['ASTERISK'] = 'cm-operator';
    tokenStyles['SLASH'] = 'cm-operator';
    tokenStyles['OPAREN'] = 'cm-bracket';
    tokenStyles['CPAREN'] = 'cm-bracket';
    tokenStyles['MEMBERSHIP'] = 'cm-keyword';
    tokenStyles['LE'] = 'cm-operator';
    tokenStyles['GE'] = 'cm-operator';
    tokenStyles['LT'] = 'cm-operator';
    tokenStyles['GT'] = 'cm-operator';
    tokenStyles['EQUAL'] = 'cm-operator';
    tokenStyles['DIAMOND'] = 'cm-operator';
    tokenStyles['EVAL'] = 'cm-keyword';
    tokenStyles['IF'] = 'cm-keyword';
    tokenStyles['THEN'] = 'cm-keyword';
    tokenStyles['ELSE'] = 'cm-keyword';
    tokenStyles['USING'] = 'cm-keyword';
    tokenStyles['WITH'] = 'cm-keyword';
    tokenStyles['CURRENT_DATE'] = 'cm-keyword';
    tokenStyles['ON'] = 'cm-keyword';
    tokenStyles['DROP'] = 'cm-keyword';
    tokenStyles['KEEP'] = 'cm-keyword';
    tokenStyles['CALC'] = 'cm-keyword';
    tokenStyles['ATTRCALC'] = 'cm-keyword';
    tokenStyles['RENAME'] = 'cm-keyword';
    tokenStyles['AS'] = 'cm-keyword';
    tokenStyles['AND'] = 'cm-operator';
    tokenStyles['OR'] = 'cm-operator';
    tokenStyles['XOR'] = 'cm-operator';
    tokenStyles['NOT'] = 'cm-operator';
    tokenStyles['BETWEEN'] = 'cm-keyword';
    tokenStyles['IN'] = 'cm-operator';
    tokenStyles['NOT_IN'] = 'cm-operator';
    tokenStyles['ISNULL'] = 'cm-keyword';
    tokenStyles['EX'] = 'cm-keyword';
    tokenStyles['UNION'] = 'cm-keyword';
    tokenStyles['DIFF'] = 'cm-keyword';
    tokenStyles['SYMDIFF'] = 'cm-keyword';
    tokenStyles['INTERSECT'] = 'cm-keyword';
    tokenStyles['KEYS'] = 'cm-keyword';
    tokenStyles['COMMA'] = 'cm-keyword';
    tokenStyles['INTYEAR'] = 'cm-keyword';
    tokenStyles['INTMONTH'] = 'cm-keyword';
    tokenStyles['INTDAY'] = 'cm-keyword';
    tokenStyles['CHECK'] = 'cm-keyword';
    tokenStyles['EXISTS_IN'] = 'cm-keyword';
    tokenStyles['TO'] = 'cm-keyword';
    tokenStyles['RETURN'] = 'cm-keyword';
    tokenStyles['IMBALANCE'] = 'cm-keyword';
    tokenStyles['ERRORCODE'] = 'cm-keyword';
    tokenStyles['ALL'] = 'cm-keyword';
    tokenStyles['AGGREGATE'] = 'cm-keyword';
    tokenStyles['ERRORLEVEL'] = 'cm-keyword';
    tokenStyles['ORDER'] = 'cm-keyword';
    tokenStyles['BY'] = 'cm-keyword';
    tokenStyles['RANK'] = 'cm-keyword';
    tokenStyles['ASC'] = 'cm-keyword';
    tokenStyles['DESC'] = 'cm-keyword';
    tokenStyles['MIN'] = 'cm-keyword';
    tokenStyles['MAX'] = 'cm-keyword';
    tokenStyles['FIRST'] = 'cm-keyword';
    tokenStyles['LAST'] = 'cm-keyword';
    tokenStyles['INDEXOF'] = 'cm-keyword';
    tokenStyles['ABS'] = 'cm-keyword';
    tokenStyles['KEY'] = 'cm-keyword';
    tokenStyles['LN'] = 'cm-keyword';
    tokenStyles['LOG'] = 'cm-keyword';
    tokenStyles['TRUNC'] = 'cm-keyword';
    tokenStyles['ROUND'] = 'cm-keyword';
    tokenStyles['POWER'] = 'cm-keyword';
    tokenStyles['MOD'] = 'cm-keyword';
    tokenStyles['LEN'] = 'cm-keyword';
    tokenStyles['CONCAT'] = 'cm-keyword';
    tokenStyles['TRIM'] = 'cm-keyword';
    tokenStyles['UCASE'] = 'cm-keyword';
    tokenStyles['LCASE'] = 'cm-keyword';
    tokenStyles['SUBSTR'] = 'cm-keyword';
    tokenStyles['SUM'] = 'cm-keyword';
    tokenStyles['AVG'] = 'cm-keyword';
    tokenStyles['MEDIAN'] = 'cm-keyword';
    tokenStyles['COUNT'] = 'cm-keyword';
    tokenStyles['DIMENSION'] = 'cm-keyword';
    tokenStyles['MEASURE'] = 'cm-keyword';
    tokenStyles['ATTRIBUTE'] = 'cm-keyword';
    tokenStyles['FILTER'] = 'cm-keyword';
    tokenStyles['MERGE'] = 'cm-keyword';
    tokenStyles['EXP'] = 'cm-keyword';
    tokenStyles['ROLE'] = 'cm-keyword';
    tokenStyles['VIRAL'] = 'cm-keyword';
    tokenStyles['CHARSET_MATCH'] = 'cm-keyword';
    tokenStyles['TYPE'] = 'cm-keyword';
    tokenStyles['NVL'] = 'cm-keyword';
    tokenStyles['HIERARCHY'] = 'cm-keyword';
    tokenStyles['UNDERSCORE'] = 'cm-keyword';
    tokenStyles['INVALID'] = 'cm-keyword';
    tokenStyles['VALUE_DOMAIN'] = 'cm-keyword';
    tokenStyles['VARIABLE'] = 'cm-keyword';
    tokenStyles['DATA'] = 'cm-keyword';
    tokenStyles['STRUCTURE'] = 'cm-keyword';
    tokenStyles['DATASET'] = 'cm-keyword';
    tokenStyles['OPERATOR'] = 'cm-keyword';
    tokenStyles['DEFINE'] = 'cm-keyword';
    tokenStyles['PUT_SYMBOL'] = 'cm-keyword';
    tokenStyles['DATAPOINT'] = 'cm-keyword';
    tokenStyles['HIERARCHICAL'] = 'cm-keyword';
    tokenStyles['RULESET'] = 'cm-keyword';
    tokenStyles['RULE'] = 'cm-keyword';
    tokenStyles['END'] = 'cm-keyword';
    tokenStyles['ALTER_DATASET'] = 'cm-keyword';
    tokenStyles['LTRIM'] = 'cm-keyword';
    tokenStyles['RTRIM'] = 'cm-keyword';
    tokenStyles['INSTR'] = 'cm-keyword';
    tokenStyles['REPLACE'] = 'cm-keyword';
    tokenStyles['CEIL'] = 'cm-keyword';
    tokenStyles['FLOOR'] = 'cm-keyword';
    tokenStyles['SQRT'] = 'cm-keyword';
    tokenStyles['ANY'] = 'cm-keyword';
    tokenStyles['SETDIFF'] = 'cm-keyword';
    tokenStyles['STDDEV_POP'] = 'cm-keyword';
    tokenStyles['STDDEV_SAMP'] = 'cm-keyword';
    tokenStyles['VAR_POP'] = 'cm-keyword';
    tokenStyles['VAR_SAMP'] = 'cm-keyword';
    tokenStyles['GROUP'] = 'cm-keyword';
    tokenStyles['EXCEPT'] = 'cm-keyword';
    tokenStyles['HAVING'] = 'cm-keyword';
    tokenStyles['FIRST_VALUE'] = 'cm-keyword';
    tokenStyles['LAST_VALUE'] = 'cm-keyword';
    tokenStyles['LAG'] = 'cm-keyword';
    tokenStyles['LEAD'] = 'cm-keyword';
    tokenStyles['RATIO_TO_REPORT'] = 'cm-keyword';
    tokenStyles['OVER'] = 'cm-keyword';
    tokenStyles['PRECEDING'] = 'cm-keyword';
    tokenStyles['FOLLOWING'] = 'cm-keyword';
    tokenStyles['UNBOUNDED'] = 'cm-keyword';
    tokenStyles['PARTITION'] = 'cm-keyword';
    tokenStyles['ROWS'] = 'cm-keyword';
    tokenStyles['RANGE'] = 'cm-keyword';
    tokenStyles['CURRENT'] = 'cm-keyword';
    tokenStyles['VALID'] = 'cm-keyword';
    tokenStyles['FILL_TIME_SERIES'] = 'cm-keyword';
    tokenStyles['FLOW_TO_STOCK'] = 'cm-keyword';
    tokenStyles['STOCK_TO_FLOW'] = 'cm-keyword';
    tokenStyles['TIMESHIFT'] = 'cm-keyword';
    tokenStyles['MEASURES'] = 'cm-keyword';
    tokenStyles['NO_MEASURES'] = 'cm-keyword';
    tokenStyles['CONDITION'] = 'cm-keyword';
    tokenStyles['BOOLEAN'] = 'cm-keyword';
    tokenStyles['DATE'] = 'cm-keyword';
    tokenStyles['TIME_PERIOD'] = 'cm-keyword';
    tokenStyles['NUMBER'] = 'cm-keyword';
    tokenStyles['STRING'] = 'cm-keyword';
    tokenStyles['INTEGER'] = 'cm-keyword';
    tokenStyles['FLOAT'] = 'cm-keyword';
    tokenStyles['LIST'] = 'cm-keyword';
    tokenStyles['RECORD'] = 'cm-keyword';
    tokenStyles['RESTRICT'] = 'cm-keyword';
    tokenStyles['YYYY'] = 'cm-keyword';
    tokenStyles['MM'] = 'cm-keyword';
    tokenStyles['DD'] = 'cm-keyword';
    tokenStyles['MAX_LENGTH'] = 'cm-keyword';
    tokenStyles['REGEXP'] = 'cm-keyword';
    tokenStyles['IS'] = 'cm-keyword';
    tokenStyles['WHEN'] = 'cm-keyword';
    tokenStyles['FROM'] = 'cm-keyword';
    tokenStyles['AGGREGATES'] = 'cm-keyword';
    tokenStyles['POINTS'] = 'cm-keyword';
    tokenStyles['POINT'] = 'cm-keyword';
    tokenStyles['TOTAL'] = 'cm-keyword';
    tokenStyles['PARTIAL'] = 'cm-keyword';
    tokenStyles['ALWAYS'] = 'cm-keyword';
    tokenStyles['INNER_JOIN'] = 'cm-keyword';
    tokenStyles['LEFT_JOIN'] = 'cm-keyword';
    tokenStyles['CROSS_JOIN'] = 'cm-keyword';
    tokenStyles['FULL_JOIN'] = 'cm-keyword';
    tokenStyles['MAPS_FROM'] = 'cm-keyword';
    tokenStyles['MAPS_TO'] = 'cm-keyword';
    tokenStyles['MAP_TO'] = 'cm-keyword';
    tokenStyles['MAP_FROM'] = 'cm-keyword';
    tokenStyles['RETURNS'] = 'cm-keyword';
    tokenStyles['PIVOT'] = 'cm-keyword';
    tokenStyles['UNPIVOT'] = 'cm-keyword';
    tokenStyles['SUBSPACE'] = 'cm-keyword';
    tokenStyles['APPLY'] = 'cm-keyword';
    tokenStyles['CONDITIONED'] = 'cm-keyword';
    tokenStyles['PERIOD_INDICATOR'] = 'cm-keyword';
    tokenStyles['SINGLE'] = 'cm-keyword';
    tokenStyles['DURATION'] = 'cm-keyword';
    tokenStyles['TIME_AGG'] = 'cm-keyword';
    tokenStyles['UNIT'] = 'cm-keyword';
    tokenStyles['VALUE'] = 'cm-keyword';
    tokenStyles['VALUEDOMAINS'] = 'cm-keyword';
    tokenStyles['VARIABLES'] = 'cm-keyword';
    tokenStyles['INPUT'] = 'cm-keyword';
    tokenStyles['OUTPUT'] = 'cm-keyword';
    tokenStyles['CAST'] = 'cm-keyword';
    tokenStyles['RULE_PRIORITY'] = 'cm-keyword';
    tokenStyles['DATASET_PRIORITY'] = 'cm-keyword';
    tokenStyles['DEFAULT'] = 'cm-keyword';
    tokenStyles['CHECK_DATAPOINT'] = 'cm-keyword';
    tokenStyles['CHECK_HIERARCHY'] = 'cm-keyword';
    tokenStyles['COMPUTED'] = 'cm-keyword';
    tokenStyles['NON_NULL'] = 'cm-keyword';
    tokenStyles['NON_ZERO'] = 'cm-keyword';
    tokenStyles['PARTIAL_NULL'] = 'cm-keyword';
    tokenStyles['PARTIAL_ZERO'] = 'cm-keyword';
    tokenStyles['ALWAYS_NULL'] = 'cm-keyword';
    tokenStyles['ALWAYS_ZERO'] = 'cm-keyword';
    tokenStyles['COMPONENTS'] = 'cm-keyword';
    tokenStyles['ALL_MEASURES'] = 'cm-keyword';
    tokenStyles['SCALAR'] = 'cm-keyword';
    tokenStyles['COMPONENT'] = 'cm-keyword';
    tokenStyles['DATAPOINT_ON_VD'] = 'cm-keyword';
    tokenStyles['DATAPOINT_ON_VAR'] = 'cm-keyword';
    tokenStyles['HIERARCHICAL_ON_VD'] = 'cm-keyword';
    tokenStyles['HIERARCHICAL_ON_VAR'] = 'cm-keyword';
    tokenStyles['SET'] = 'cm-keyword';
    tokenStyles['LANGUAGE'] = 'cm-keyword';
    tokenStyles['TRUE'] = 'cm-keyword';
    tokenStyles['FALSE'] = 'cm-keyword';
    tokenStyles['INTEGER_CONSTANT'] = 'cm-number';
    tokenStyles['FLOAT_CONSTANT'] = 'cm-number';
    tokenStyles['NULL_CONSTANT'] = 'cm-keyword';
    tokenStyles['STRING_CONSTANT'] = 'cm-string';
    tokenStyles['IDENTIFIER'] = 'cm-variable';
    //tokenStyles['FREQUENCY'] = 'cm-keyword';
    //tokenStyles['MONTH'] = 'cm-keyword';
    //tokenStyles['DAY'] = 'cm-keyword';
    //tokenStyles['YEAR'] = 'cm-keyword';
    //tokenStyles['WEEK'] = 'cm-keyword';
    //tokenStyles['HOURS'] = 'cm-keyword';
    //tokenStyles['MINUTES'] = 'cm-keyword';
    //tokenStyles['SECONDS'] = 'cm-keyword';
    //tokenStyles['DATE_FORMAT'] = 'cm-keyword';
    //tokenStyles['TIME_FORMAT'] = 'cm-keyword';
    //tokenStyles['TIME'] = 'cm-keyword';
    //tokenStyles['WS'] = 'cm-comment';
    tokenStyles['ML_COMMENT'] = 'cm-comment';
    tokenStyles['SL_COMMENT'] = 'cm-comment';

    vtl.editor.setDefaultTokenStyles(tokenStyles);
    var el = document.getElementById('vtlwell');
    //vtl.editor.editorImplementation.setOption("scrollbarStyle", "simple");
    vtl.editor.editorImplementation.setOption("extraKeys", {
      'Ctrl-Space': function(cm) {
        cm.showHint({ hint: suggest });
      }
    });

    el.parentNode.replaceChild(vtl.editor.getDomElement(), el);
    window.CodeMirror = vtl.CodeMirror;
}());

