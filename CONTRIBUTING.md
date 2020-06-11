# Contributing to VTL Engine and Editor

:+1::tada: Thank you for your effort in contribution! :tada::+1:

The following is a set of guidelines for contributing to the project.
These are mostly guidelines, not rules. Use your best judgment, and 
feel free to propose changes to this document in a pull request.

## I just have a question!!!

> Note: **Please don't file an issue to ask a question.**.

Please read the documentation, and if you can't find your answer there, contact the developers by email.

You'll get much faster and polite answers than when filing an issue.

## Reporting Bugs

This section guides you through submitting a bug report for VTL Engine and Editor. 
Following these guidelines helps maintainers and the community understand your report :pencil:,
reproduce the behavior :computer:, and find related reports :mag_right:.

Before creating bug reports, please check [this list](#before-submitting-a-bug-report) as you might find out that you don't need to create one. When you are creating a bug report, please [include as many details as possible](#how-do-i-submit-a-good-bug-report). Fill out [the required template](https://github.com/atom/.github/blob/master/.github/ISSUE_TEMPLATE/bug_report.md), the information it asks for helps us resolve issues faster.

> **Note:** If you find a **CLOSED** issue that seems like it is the same thing that you're experiencing, 
open a new issue and include a link to the original issue in the body of your new one.

### How Do I Submit A (Good) Bug Report?

Bugs are tracked as [GitHub issues](https://guides.github.com/features/issues/). 
You can create an issue on that repository by filling in 
[the template](https://github.com/vpinna80/VTL/issues/new?assignees=&labels=bug&template=bug_report.md&title=).

Explain the problem and include any additional details that could help maintainers reproduce the problem.
These are some suggestions:

* Use a **clear and descriptive title** for the issue to identify the problem.
* **Describe the exact steps** which reproduce the problem in as many details as possible.
* **Provide specific examples to demonstrate the steps**. Include links to files or GitHub projects, or copy/pasteable snippets, which you use in those examples. If you're providing snippets in the issue, use [Markdown code blocks](https://help.github.com/articles/markdown-basics/#multiple-lines).
* Describe the **behavior you observed after following the steps** and point out what exactly is **the problem with that behavior**.
* **Explain which behavior you expected to see instead and why.**
* **Include screenshots and animated GIFs** which show you following the described steps and clearly demonstrate the problem. You can use [this tool](https://www.cockos.com/licecap/) to record GIFs on macOS and Windows, and [this tool](https://github.com/colinkeenan/silentcast) or [this tool](https://github.com/GNOME/byzanz) on Linux.
* **If you're reporting that VTL Engine and Editor crashed**, include a crash report with a stack trace from the operating system. On macOS, the crash report will be available in `Console.app` under "Diagnostic and usage information" > "User diagnostic reports". Include the crash report in the issue in a [code block](https://help.github.com/articles/markdown-basics/#multiple-lines), a [file attachment](https://help.github.com/articles/file-attachments-on-issues-and-pull-requests/), or put it in a [gist](https://gist.github.com/) and provide link to that gist.
* **If the problem is related to performance or memory**, include a CPU profile capture with your report.
* **If the problem wasn't triggered by a specific action**, describe what you were doing before the problem happened and share more information using the guidelines below.

Provide more context by answering these questions:

* **Did the problem start happening recently** (e.g. after updating to a new version of Atom) or was this always a problem?
* If the problem started happening recently, **can you reproduce the problem in an older version?** You can download older versions from [GitHub Releases](https://github.com/vpinna80/VTL/releases).
* **Does the problem happen consistently or randomly?** Provide details about how often the problem happens and under which conditions it normally happens.

Include details about your configuration and environment:

* **The version of VTL Engine and Editor** which you are you using.
* **The name and version of the OS** you're using.
* **The Jaa enironment** you're using.

## Proposing new features

This section guides you through submitting a feature request for the VTL Engine and Editor, 
including completely new features and minor improvements to existing functionality. 
Following these guidelines helps maintainers to better understand your suggestion :pencil:.

### How Do I Submit A (Good) Enhancement Suggestion?

Enhancement suggestions are tracked in the [GitHub issues page](https://github.com/vpinna80/VTL/issues?q=is%3Aopen+is%3Aissue+label%3Aenhancement).
You can create an issue for a feature request filling [this form](https://github.com/vpinna80/VTL/issues/new?assignees=&labels=&template=feature_request.md&title=).

* Use a **clear and descriptive title** for the issue to identify the suggestion.
* Provide a **step-by-step description of the suggested enhancement** in as many details as possible.
* Provide **specific examples to demonstrate the steps**. Include copy/pasteable snippets which you use in those examples, as [Markdown code blocks](https://help.github.com/articles/markdown-basics/#multiple-lines).
* **Describe the current behavior** and **explain which behavior you expected to see instead** and why.
* **Include screenshots and animated GIFs** which help you demonstrate the steps.
* Explain why **this enhancement would be useful to the community**.
* Specify **which version of VTL Emngine and Editor** you're using.

## Pull Requests

The process described here has several goals:

- Maintain VTL Engine and Editor's quality;
- Fix problems that are important to users;
- Engage the community in working together;
- Helping maintainers reviewing the proposals.

### Pull request style

* Limit the commit message to a single line of no more than 90 characters;
* Split your work into multiple commits where each commit solve a specific problem;
* In a single commit apply only the minimum amount of changes needed to solve the problem; 
* [Reference](https://help.github.com/en/github/writing-on-github/autolinked-references-and-urls#issues-and-pull-requests) issues and pull requests;
* Be sure to rebase your pull request to the current tip of master branch.

The maintainers may at any time ask you to complete additional design work, 
tests, or other changes before your pull request can be ultimately accepted.
