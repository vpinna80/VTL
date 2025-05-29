///
/// Copyright © 2020 Banca D'Italia
///
/// Licensed under the EUPL, Version 1.2 (the "License");
/// You may not use this work except in compliance with the
/// License.
/// You may obtain a copy of the License at:
///
/// https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
///
/// Unless required by applicable law or agreed to in
/// writing, software distributed under the License is
/// distributed on an "AS IS" basis,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
/// express or implied.
///
/// See the License for the specific language governing
/// permissions and limitations under the License.
///

import { Compartment, SelectionRange, EditorState } from "@codemirror/state"
import { EditorView, keymap, lineNumbers, highlightActiveLineGutter, highlightSpecialChars, drawSelection, dropCursor, crosshairCursor, highlightActiveLine } from "@codemirror/view"
import { defaultKeymap, history, historyKeymap } from "@codemirror/commands"
import { linter, lintKeymap, Diagnostic, Action } from "@codemirror/lint"
import { searchKeymap, search, highlightSelectionMatches } from "@codemirror/search"
import { closeBrackets, autocompletion, closeBracketsKeymap, completionKeymap, CompletionContext } from "@codemirror/autocomplete"
import { syntaxTree, Language, defineLanguageFacet, foldGutter, indentOnInput, bracketMatching, foldKeymap, languageDataProp } from "@codemirror/language"
import { Parser as LezerParser, NodeType, NodeSet, Tree, SyntaxNodeRef, NodeProp } from "@lezer/common"
import { styleTags, tags } from "@lezer/highlight"
import { default as VTLParser, StartContext, ComponentIDContext, ClauseExprContext, VarIDContext, DatasetClauseContext } from "./VtlParser"
import { default as VTLLexer } from "./VtlLexer"
import antlr4 from 'antlr4'
import * as th from '@uiw/codemirror-themes-all'
import { TokenSource } from "antlr4/src/antlr4/TokenSource"

let tokenMapper = {
	comment: ["ML_COMMENT", "SL_COMMENT"],
	name: ["IDENTIFIER"],
	string: ["STRING_CONSTANT"],
	integer: ["INTEGER_CONSTANT"],
	float: ["NUMBER_CONSTANT"],
	bool: ["BOOLEAN_CONSTANT"],
	angleBracket: ["LT", "GT"],
	squareBracket: ["QLPAREN", "QRPAREN"],
	paren: ["LPAREN", "RPAREN"],
	brace: ["GLPAREN", "GRPAREN"],
	null: ["NULL_CONSTANT"],
	operator: ["MEMBERSHIP"],
	arithmeticOperator: ["PLUS", "MINUS", "MUL", "DIV"],
	logicOperator: ["AND", "OR", "XOR", "NOT"],
	compareOperator: ["EQ", "LT", "MT", "ME", "NEQ", "LE"],
	definitionOperator: ["ASSIGN", "POINTER"],
	definitionKeyword: ["DEFINE", "DATAPOINT", "HIERARCHICAL", "RULESET", "AS", "IS"],
	operatorKeyword: ["CURRENT_DATE", "BETWEEN", "IN", "NOT_IN", "ISNULL", "EXISTS_IN", "RANK", "MIN", "MAX", "INDEXOF", "ABS", "LN", "LOG", "TRUNC",
		"ROUND", "POWER", "MOD", "LEN", "CONCAT", "TRIM", "UCASE", "LCASE", "SUBSTR", "SUM", "AVG", "MEDIAN", "COUNT", "EXP", "CHARSET_MATCH",
		"NVL", "LTRIM", "RTRIM", "INSTR", "REPLACE", "CEIL", "FLOOR", "SQRT", "STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP", "FIRST_VALUE",
		"LAST_VALUE", "LAG", "LEAD", "RATIO_TO_REPORT", "FILL_TIME_SERIES", "FLOW_TO_STOCK", "STOCK_TO_FLOW", "TIMESHIFT", "INNER_JOIN",
		"LEFT_JOIN", "CROSS_JOIN", "FULL_JOIN", "TIME_AGG", "CAST", "EVAL", "PERIOD_INDICATOR", "SETDIFF", "UNION", "SYMDIFF", "INTERSECT"],
	controlKeyword: ["IF", "THEN", "ELSE", "CALC", "DROP", "KEEP", "CALC", "RENAME", "AGGREGATE", "FILTER", "PIVOT", "UNPIVOT", "SUBSPACE"],
	typeOperator: ["DIMENSION", "MEASURE", "ATTRIBUTE", "VIRAL", "BOOLEAN", "DATE", "TIME_PERIOD", "NUMBER", "STRING", "TIME", "INTEGER", "FLOAT", "LIST"],
	controlOperator: ["COMMA", "COLON", "ORDER", "BY", "ASC", "DESC", "GROUP", "EXCEPT", "HAVING", "OVER", "PRECEDING", "FOLLOWING", "UNBOUNDED", "PARTITION",
		"ROWS", "RANGE", "CURRENT", "DATAPOINT"]
}

let mapperInverter = function(mapper: Object) {
	let inverted = {}
	Object.getOwnPropertyNames(mapper).forEach(p => mapper[p].forEach((n: string) => {
		if (inverted[n]) {
			inverted[n].push(tags[p])
		} else {
			inverted[n] = [tags[p]]
		}
	}))
	return inverted
}

// Props added to nodes containing additional info for the editor renderer
let ctxProp = new NodeProp<antlr4.tree.ParseTree>({ perNode: true }) // The RuleContext or TerminalNode
let idsProp = new NodeProp({ perNode: true })
let dsNameProp = new NodeProp<string>({ perNode: true }) // The dataset name when using # or []
let expectedProp = new NodeProp({ perNode: true }) // Error: tokens expected but not encountered
let missingAfterProp = new NodeProp({ perNode: true }) // Error: Missing token after another token
let offendingTokenProp = new NodeProp({ perNode: true }) // Error: Unexpected token encountered

let completer = (src: CompletionContext) => { 
	let range = src.state.wordAt(src.pos)
	let topNode = syntaxTree(src.state)
	let from = range && range.from || src.pos, to = range && range.to || src.pos
	let node = topNode.resolveInner(to, -1).toTree()
	let ctx: antlr4.tree.ParseTree = node.prop(ctxProp)
	if (ctx instanceof antlr4.tree.TerminalNode) {
		if (ctx.getSymbol().type == VTLLexer.IDENTIFIER) {
			return {
				from: from,
				options: [...((topNode.prop(idsProp) as Object)[node.prop(dsNameProp) as string] || [])].map((c: string) => { return { label: c }}),
				validFor: /'[^']+'|[a-zA-Z0-9_]+/
			}
		}
	}}

let facet = defineLanguageFacet({
	"autocomplete": completer,
	"closeBrackets": {}
})

let EOF = NodeType.define({ id: 0, name: "ANTLR_EOF", top: false, error: false, skipped: true })
let tokensStartIndex = 1
let tokenSet = (VTLLexer.symbolicNames.slice(1) as string[]).map((n, i) => NodeType.define({ id: i + tokensStartIndex, name: n, top: false, error: false, skipped: false }))
let rulesStartIndex = 1 + tokenSet.length
let ruleSet = VTLParser.ruleNames.map((n, i) => NodeType.define({ id: i + rulesStartIndex, name: n, top: !i, error: false, skipped: false, props: !i && [[languageDataProp, facet]] || []}))
let errorStartIndex = rulesStartIndex + ruleSet.length
let errorTokens = {
	VTL_EXTRANEOUS_TOKEN: errorStartIndex + 0, 
	VTL_MISSING_TOKEN: errorStartIndex + 1,
	VTL_TOKEN_MISMATCH: errorStartIndex + 2
}
let errorSet = Object.getOwnPropertyNames(errorTokens).map((n: string) => NodeType.define({ id: errorTokens[n], name: n, top: false, error: true, skipped: false }))
let nodeset = new NodeSet([EOF, ...tokenSet, ...ruleSet, ...errorSet]).extend(styleTags(mapperInverter(tokenMapper)))

let addCommentTokens = function(tokens: antlr4.Token[]) {
	let children: [number, Tree, boolean][] = []
	for (const ignored of tokens) {
		if ([VTLLexer.ML_COMMENT, VTLLexer.SL_COMMENT].includes(ignored.type)) {
			children = [...children, [ignored.start, new Tree(nodeset.types[ignored.type], [], [], ignored.stop - ignored.start + 1), false]]
		}
	}
	return children
}
		
let createTree = function(text: string, ctxName: string = 'start') {
	let lexer = new VTLLexer(new antlr4.CharStream(text))
	lexer.removeErrorListeners()
	let parser = new VTLParser(new antlr4.CommonTokenStream(lexer))
	parser.removeErrorListeners()
		
	// capture variables
	let lastToken: antlr4.Token
	let ids: { [key: string]: Set<string> } = { }
	
	let treeToArray = function(ctx: antlr4.tree.ParseTree, dsName = ""): [number, Tree, boolean] {
		if (ctx instanceof antlr4.tree.RuleNode) {
			if (ctx instanceof StartContext && ctx.getSourceInterval() === antlr4.misc.Interval.INVALID_INTERVAL) {
				return [0, new Tree(EOF, [], [], text.length), false]
			}
			let tokenStream = parser._input as antlr4.CommonTokenStream
			let start: number = ctx.start.start
			if (ctx instanceof DatasetClauseContext) {
				dsName = (ctx.parentCtx as ClauseExprContext).expr().getText()
			} else if (ctx instanceof VarIDContext || ctx instanceof ComponentIDContext) {
				if (!ids[dsName]) {
					ids[dsName] = new Set<string>()
				}
				ids[dsName].add(ctx.getText())
			}
			
			if (ctx.exception) {
				if (ctx.exception instanceof antlr4.error.InputMismatchException) {
					let offToken = ctx.exception.offendingToken
					let expected = ctx.exception.getExpectedTokens()
					let tokens = Array(expected.intervals.at(-1).stop).keys().filter(i => expected.contains(i)).map(i => VTLLexer.literalNames[i] || VTLLexer.symbolicNames[i]).filter(e => !!e).toArray()
					let mismatched = new Tree(nodeset.types[errorTokens.VTL_TOKEN_MISMATCH], [], [], offToken.stop - offToken.start + 1, [[expectedProp, tokens]])
					let children: [number, Tree, boolean][] = ctx.children && ctx.children.map((c: antlr4.tree.ParseTree) => treeToArray(c, dsName)) || []
					if (!children.some(c => c[2])) {
						children = [...children, [offToken.start, mismatched, true]]
					}
					let tchildren: [number[], Tree[]] = [children.map(r => r[0] - start), children.map(r => r[1])]
					return [start, new Tree(nodeset.types[ctx.ruleIndex + rulesStartIndex], tchildren[1], tchildren[0], ctx.start.stop - start + 1, [[ctxProp, ctx]]), true]
				} else if (ctx.exception instanceof antlr4.error.NoViableAltException) {
					let noviable = new Tree(nodeset.types[errorTokens.VTL_EXTRANEOUS_TOKEN], [], [], 1, [[offendingTokenProp, "tokens"]])
					lastToken = ctx.start
					return [start, noviable, true];
				} else {
					throw ctx.exception
				}
			} else {
				let children: [number, Tree, boolean][] = []
				if (ctx instanceof StartContext) {
					start = 0
				} else if (ctx.start.tokenIndex > 0) {
					// fetch ws - comment tokens before each sub-rule
					let startIndex = lastToken && lastToken.tokenIndex + 1 || 0
					children = [...children, ...addCommentTokens(tokenStream.getTokens(startIndex, ctx.start.tokenIndex))]
				}
				for (let i = 0; i < ctx.children.length; i++) {
					let lastIndex = lastToken && lastToken.tokenIndex + 1 || 0
					let child = treeToArray(ctx.children[i], dsName)
					if (i > 0 && lastIndex && ctx.children[i] instanceof antlr4.tree.TerminalNode && !ctx.children[i].isErrorNode) {
						children = [...children, ...addCommentTokens(tokenStream.getTokens(lastIndex + 1, ctx.children[i].symbol.tokenIndex))]
					}
					children = [...children, child].filter(e => e)
				}
				// fetch eventual skipped comments at the end of the code
				if (ctx instanceof StartContext) {
					children = [...children, ...addCommentTokens(tokenStream.getTokens(ctx.stop.tokenIndex + 1, parser._input.tokens.length - 1))]
				}
				let tchildren: [number[], Tree[], boolean] = [children.map(r => r[0] - start), children.map(r => r[1]), children.some(r => r[2])]
				let props: [NodeProp<any>, any][] = ctx instanceof StartContext && [[idsProp, ids]] || []
				props.push([ctxProp, ctx])
				return [start, new Tree(nodeset.types[ctx.ruleIndex + rulesStartIndex], tchildren[1], tchildren[0], tchildren[0].at(-1) + tchildren[1].at(-1).length + 1, props), tchildren[2]]
			}
		} else if (ctx instanceof antlr4.tree.TerminalNode) {
			let token = ctx.getSymbol()
			if (ctx.isErrorNode && ctx.isErrorNode()) {
				if (token.tokenIndex == -1) { // missing token
					let missing = token.text.substring(9, token.text.length - 1)
					return [lastToken && lastToken.start || 0, new Tree(nodeset.types[errorTokens.VTL_MISSING_TOKEN], [], [], lastToken.stop - lastToken.start + 1, [[ctxProp, ctx], [missingAfterProp, lastToken.text], [expectedProp, missing]]), true]
				}
				lastToken = token
				return [token.start, new Tree(nodeset.types[errorTokens.VTL_EXTRANEOUS_TOKEN], [], [], token.stop - token.start + 1, [[dsNameProp, dsName], [ctxProp, ctx], [offendingTokenProp, token.text]]), true]
			} else {
				lastToken = token
				return [token.start, new Tree(nodeset.types[token.type === -1 ? 0 : token.type], [], [], token.stop - token.start + 1, [[dsNameProp, dsName], [ctxProp, ctx]]), false]
			}
		} else {
			throw new Error("Unrecognized node type")
		}
	}
	
	let tree = treeToArray(parser[ctxName.charAt(0).toLowerCase() + ctxName.slice(1)].bind(parser)())[1]
	return tree
}

let VTLLang = new Language(facet, new class extends LezerParser {
	createParse(input) {
		let doc = input.doc.text.join("\n")
		let tree = createTree(doc)
		let partialParse = {
			parsedPos: doc.length,
			stoppedAt: null,
			stopAt: () => null,
			advance: () => tree
		}
		return partialParse
	}
}(), [], "VTL")

let encode = (str: any) => {
	let pre = document.createElement('pre');
    pre.appendChild(document.createTextNode(str.toString()));
    return pre.innerHTML;
}

let vtlLinter = linter((view: EditorView) => {
	let diagnostics: Diagnostic[] = []
	syntaxTree(view.state).cursor().iterate((node: SyntaxNodeRef) => {
		if (node.type.id >= errorStartIndex) {
			const tree = node.tree as Tree
			let msg = ''
			let actions: Action[] = []
			switch (node.name) {
				case "VTL_EXTRANEOUS_TOKEN": {
					msg = `Unexpected token: <code>${tree.prop(offendingTokenProp)}</code>`
					actions.push({
						name: "Remove",
						apply: (view, from, to) => { view.dispatch({ changes: { from, to } }) }
					})
					break;
				}
				case "VTL_MISSING_TOKEN": {
					msg = `Missing <code>${encode(tree.prop(expectedProp))}</code> after <code>${tree.prop(missingAfterProp)}<code>`
					break
				}
				case "VTL_TOKEN_MISMATCH": {
 					msg = `Expected: <code>${tree.prop(expectedProp)}</code>`
					break
				}
			}
			diagnostics.push({ from: node.from, to: node.to, severity: "error", message: msg, renderMessage: () => {
				let el = document.createElement( 'span' );
				el.innerHTML = msg
				return el
			}, actions: actions })
		}
	})
	return diagnostics
})

let baseKeyBindings = [
	...closeBracketsKeymap,
	...defaultKeymap,
	...searchKeymap,
	...historyKeymap,
	...foldKeymap,
	...completionKeymap,
	...lintKeymap
];

let extensions = [
	lineNumbers(),
	highlightActiveLineGutter(),
	highlightSpecialChars(),
	history(),
	foldGutter(),
	drawSelection(),
	dropCursor(),
	indentOnInput(),
	bracketMatching(),
	search(),
	closeBrackets(),
	autocompletion(),
	crosshairCursor(),
	highlightActiveLine(),
	highlightSelectionMatches(),
	VTLLang.extension,
	vtlLinter
]

const themeCompartment = new Compartment
const keymapCompartment = new Compartment
const fontSizeCompartment = new Compartment;
const createFontSizeTheme = (fontSize: number) => EditorView.theme({ '.cm-scroller': { fontSize: `${fontSize}pt` } })
export const views: { [name: string]: EditorView } = {}
export function createEditor(name: string, themeName?: string): EditorView {
	const theme = th[themeName!] || th.material;
	const view = new EditorView({
		state: EditorState.create({
		    extensions: [
				...extensions,
				keymapCompartment.of(keymap.of(baseKeyBindings)),
				themeCompartment.of(theme),
				fontSizeCompartment.of(createFontSizeTheme(12))
			]
		})
	})
	
	views[name] = view
	return view
}

export const themes = Object.getOwnPropertyNames(th).filter(s => !s.match("(^default|Init$)"))
export const setTheme = (themeName: string) => {
	for (const view of Object.values(views)) {
		view.dispatch({ effects: themeCompartment.reconfigure(th[themeName]) })
		view.requestMeasure();
	}
}

export const addHotKey = (view: EditorView, hotKey: string, command: (view: EditorView) => boolean) => {
	view.dispatch({ 
		effects: keymapCompartment.reconfigure(keymap.of([{
			key: hotKey, run: command
		}, ...keymapCompartment.get(view.state).value]))
	})
}

export const setFontSize = (fontSize: number) => {
	for (const view of Object.values(views)) {
		view.dispatch({ effects: fontSizeCompartment.reconfigure(createFontSizeTheme(fontSize)) })
		view.requestMeasure();
	}
}