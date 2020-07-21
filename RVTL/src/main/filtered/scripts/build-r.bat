@echo off
set filename="${project.artifactId}_${r.package.version}.tar.gz"
echo Building: %filename%
echo Working in: %cd%
if not exist target\build mkdir target\build
if ERRORLEVEL 1 goto error
cd target\build
if ERRORLEVEL 1 goto error
del /q %filename% 2> NUL:
echo roxygen2::roxygenize("..\\classes\\R") | R --vanilla -q
R CMD build ..\classes\R
if ERRORLEVEL 1 goto error
rd /s/q ${project.artifactId}.Rcheck
if /i "${maven.test.skip}" EQU "true" goto dist
R CMD check --no-manual --install-args=--no-multiarch %filename%
if ERRORLEVEL 1 goto error
:dist
copy %filename% ..
del /q %filename%
goto fine
:error
exit 1
:fine
