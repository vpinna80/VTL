--
-- Copyright © 2020 Banca D'Italia
--
-- Licensed under the EUPL, Version 1.2 (the "License");
-- You may not use this work except in compliance with the
-- License.
-- You may obtain a copy of the License at:
--
-- https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
--
-- Unless required by applicable law or agreed to in
-- writing, software distributed under the License is
-- distributed on an "AS IS" basis,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
-- express or implied.
--
-- See the License for the specific language governing
-- permissions and limitations under the License.
--

CREATE TABLE DOMAINSET
(
--   COMMUNITYID     VARCHAR (60)     NOT NULL,
   SETID           VARCHAR (60)     NOT NULL,
--   DESCRIPTION     VARCHAR (254)    NOT NULL,
--   DOMAINID        VARCHAR (60)     NOT NULL,
   STARTDATE       DATE             NOT NULL,
   ENDDATE         DATE             DEFAULT (date '9999-12-31') ,
--   ISENUMERATED    NUMERIC          NOT NULL,
--   ISBYCRITERION   NUMERIC          NOT NULL,
--   ISBOOLEAN       NUMERIC          NOT NULL,
--   ISORDINAL       NUMERIC          NOT NULL,
--   ISFULLSET       NUMERIC          NOT NULL,
--   CRITERIONTYPE   VARCHAR (20)     NOT NULL,
--   CRITERIONPARAM  VARCHAR (2636)   NOT NULL,
   CONSTRAINT pk_domainset PRIMARY KEY (
--      COMMUNITYID, 
--      DOMAINID, 
      SETID, 
      STARTDATE
   )
);

CREATE TABLE STRUCTUREITEM
(
--   COMMUNITYID    VARCHAR (60)     NOT NULL,
--   CONTEXTID      VARCHAR (60)     NOT NULL,
   CUBEID         VARCHAR (250)    NOT NULL,
   VARIABLEID     VARCHAR (60)     NOT NULL,
   SETID          VARCHAR (60),
   ROLE           VARCHAR (60)     NOT NULL,
--   SURVEYID       VARCHAR (60)     NOT NULL,
   DOMAINID       VARCHAR (60)     NOT NULL,
--   UNIQUEVALUEID  VARCHAR (1050),
--   PROPERTY       VARCHAR (3000)   NOT NULL,
--   CUBESTATTYPE   VARCHAR (20)     NOT NULL,
   STARTDATE      DATE             DEFAULT CURRENT_DATE,
   ENDDATE        DATE             DEFAULT (date '9999-12-31'),
   CONSTRAINT pk_structureitem PRIMARY KEY (
--      COMMUNITYID, 
--      CONTEXTID, 
      CUBEID, 
      VARIABLEID, 
      ROLE
   )
);
