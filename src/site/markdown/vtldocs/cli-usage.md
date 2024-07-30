# Java -jar vtl-cli.jar Manual

## Name

java -jar vtl-cli.jar - Parses a set of VTL rules and computes one or more results.

## Synopsis

<code><b>java -jar vtl-cli.jar</b> [<b>-hV</b>] [<b>-f</b> <i>file</i>] <b>[<i>rulename</i>…]</b></code>

## Description

Parses a set of VTL rules and computes one or more results.

## Options

<code><b>-f</b>, <b>\-\-file</b> <i>file</i></code>:
  Input VTL script. If not specified, read from stdin.

<code><b>-h</b>, <b>\-\-help</b></code>:
  Show this help message and exit.

<code><b>-v</b>, <b>\-\-version</b></code>:
  Print version information and exit.

## Arguments

<code>[<i>rulename</i>…]</code>:
  Rule names whose values will be printed. If none is specified, print the values of all rules.
