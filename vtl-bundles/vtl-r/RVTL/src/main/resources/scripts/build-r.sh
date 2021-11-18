#!/bin/bash
#
# Copyright © 2020 Banca D'Italia
#
# Licensed under the EUPL, Version 1.2 (the "License");
# You may not use this work except in compliance with the
# License.
# You may obtain a copy of the License at:
#
# https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
#
# Unless required by applicable law or agreed to in
# writing, software distributed under the License is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
#
# See the License for the specific language governing
# permissions and limitations under the License.
#

set -e


mkdir -p "${project.build.directory}/build"
cd "${project.build.directory}/build"
echo Working in: $PWD
FILENAME="$PWD/${project.artifactId}_${r.package.version}.tar.gz"

echo Building: $FILENAME

echo 'roxygen2::roxygenize("../classes/R")' | "${r.prepend.path}R" -q

if [ '${maven.test.skip}' != 'true' ]; then
	rm -rf ${project.artifactId}.Rcheck
	#"${r.prepend.path}R" CMD check --no-manual --install-args=--no-multiarch ${project.build.outputDirectory}/R
fi

rm -rf $FILENAME
"${r.prepend.path}R" CMD build ${project.build.outputDirectory}/R

mv $FILENAME ..

