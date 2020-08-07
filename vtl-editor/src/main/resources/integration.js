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
require("codemirror");
//require("codemirror/addon/hint/show-hint")
require("codemirror/addon/edit/closebrackets")

var editor = require("./src").createEditor();

editor.setEditorPlaceholderText("Enter your code here...");
editor.setShowLineNumbers(true);
editor.setDisplayEditorErrors(true);
editor.setTheme("idea");

merge = require("lodash.merge");
antlr = require("antlr4");
antlr.atn = merge(antlr.atn, require("antlr4/atn/ATNState"), require("antlr4/atn/Transition"))
antlr.error = merge(antlr.error, require("antlr4/error/Errors"))

module.exports = {
	editor: editor,
	antlr: antlr,
	gen: {
		VtlParser: require("./src/VtlParser").VtlParser,
		VtlLexer: require("./src/VtlLexer").VtlLexer
	},
	CodeMirror: require("codemirror"),
//	hint: require("codemirror/addon/hint/show-hint")
};
