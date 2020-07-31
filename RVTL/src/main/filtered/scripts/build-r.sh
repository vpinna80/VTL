#!/bin/bash
mkdir -p "${project.build.directory}/build"
cd "${project.build.directory}/build"
echo Working in: $PWD
FILENAME="$PWD/${project.artifactId}_${r.package.version}.tar.gz"

echo Building: $FILENAME

echo 'roxygen2::roxygenize("../classes/R")' | "${r.prepend.path}R" --vanilla -q

if [ "${maven.test.skip}" != "true" ]; then
	rm -rf ${project.artifactId}.Rcheck
	#"${r.prepend.path}R" CMD check --no-manual --install-args=--no-multiarch ${project.build.outputDirectory}/R
fi

rm -rf $FILENAME
"${r.prepend.path}R" CMD build ${project.build.outputDirectory}/R

mv $FILENAME ..

