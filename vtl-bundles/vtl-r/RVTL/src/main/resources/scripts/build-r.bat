@REM
@REM Copyright © 2020 Banca D'Italia
@REM
@REM Licensed under the EUPL, Version 1.2 (the "License");
@REM You may not use this work except in compliance with the
@REM License.
@REM You may obtain a copy of the License at:
@REM
@REM https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
@REM
@REM Unless required by applicable law or agreed to in
@REM writing, software distributed under the License is
@REM distributed on an "AS IS" basis,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
@REM express or implied.
@REM
@REM See the License for the specific language governing
@REM permissions and limitations under the License.
@REM

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
if ERRORLEVEL 1 goto error
R CMD build ..\classes\R
if ERRORLEVEL 1 goto error
rd /s/q ${project.artifactId}.Rcheck
if /i "${maven.test.skip}" EQU "true" goto dist
if /i "${skipTests}" EQU "true" goto dist
REM R CMD check --no-manual --install-args=--no-multiarch %filename%
if ERRORLEVEL 1 goto error
:dist
copy %filename% ..
del /q %filename%
goto fine
:error
exit 1
:fine
