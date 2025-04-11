lexer grammar VtlTokens;


  PUT_SYMBOL          : '<-';
  LPAREN              : '(';
  RPAREN              : ')';
  QLPAREN             : '[';
  QRPAREN             : ']';
  GLPAREN             : '{';
  GRPAREN             : '}';
  EQ                  : '=';
  LT                  : '<';
  MT                  : '>';
  ME                  : '>=';
  NEQ                 : '<>';
  LE                  : '<=';
  PLUS                : '+';
  MINUS               : '-';
  MUL                 : '*';
  DIV                 : '/';
  COMMA               : ',';
  POINTER             : '->';
  COLON               : ':';
  ASSIGN              : ':=';
  MEMBERSHIP          : '#';
  OPTIONAL            : '_';
  CONCAT              : '||';
  
  ABS                 : 'abs';
  AGGREGATE           : 'aggr';
  AGGREGATES          : 'aggregates';
  ALL                 : 'all';
  ALL_MEASURES        : 'all_measures';
  ALTER_DATASET       : 'alterDataset';
  ALWAYS              : 'always';
  ALWAYS_NULL         : 'always_null';
  ALWAYS_ZERO         : 'always_zero';
  AND                 : 'and';
  ANY                 : 'any';
  APPLY               : 'apply';
  AS                  : 'as';
  ASC                 : 'asc';
  ATTRCALC            : 'attrcalc';
  ATTRIBUTE           : 'attribute';
  AVG                 : 'avg';
  BETWEEN             : 'between';
  BOOLEAN             : 'boolean';
  BY                  : 'by';
  CALC                : 'calc';
  CASE                : 'case';
  CAST                : 'cast';
  CEIL                : 'ceil';
  CHARSET_MATCH       : 'match_characters';
  CHECK               : 'check';
  CHECK_DATAPOINT     : 'check_datapoint';
  CHECK_HIERARCHY     : 'check_hierarchy';
  COMPONENT           : 'component';
  COMPONENTS          : 'components';
  COMPUTED            : 'computed';
  CONDITION           : 'condition';
  CONDITIONED         : 'conditioned';
  COUNT               : 'count';
  CROSS_JOIN          : 'cross_join';
  CURRENT             : 'current';
  CURRENT_DATE        : 'current_date';
  CUSTOMPIVOT         : 'customPivot';
  DATA                : 'data';
  DATAPOINT           : 'datapoint';
  DATAPOINT_ON_VAR    : 'datapoint_on_variables';
  DATAPOINT_ON_VD     : 'datapoint_on_valuedomains';
  DATASET             : 'dataset';
  DATASET_PRIORITY    : 'dataset_priority';
  DATE                : 'date';
  DATEADD             : 'dateadd';
  DATEDIFF            : 'datediff';
  DAYOFMONTH          : 'dayofmonth';
  DAYOFYEAR           : 'dayofyear';
  DAYTOMONTH          : 'daytomonth';
  DAYTOYEAR           : 'daytoyear';
  DD                  : 'dd';
  DEFAULT             : 'default';
  DEFINE              : 'define';
  DESC                : 'desc';
  DIFF                : 'diff';
  DIMENSION           : 'identifier';
  DROP                : 'drop';
  DURATION            : 'duration';
  ELSE                : 'else';
  END                 : 'end';
  ERRORCODE           : 'errorcode';
  ERRORLEVEL          : 'errorlevel';
  EVAL                : 'eval';
  EX                  : 'ex';
  EXCEPT              : 'except';
  EXISTS_IN           : 'exists_in';
  EXP                 : 'exp';
  FILL_TIME_SERIES    : 'fill_time_series';
  FILTER              : 'filter';
  FIRST               : 'first';
  FIRST_VALUE         : 'first_value';
  FLOAT               : 'float';
  FLOOR               : 'floor';
  FLOW_TO_STOCK       : 'flow_to_stock';
  FOLLOWING           : 'following';
  FROM                : 'from';
  FULL_JOIN           : 'full_join';
  GROUP               : 'group';
  HAVING              : 'having';
  HIERARCHICAL        : 'hierarchical';
  HIERARCHICAL_ON_VAR : 'hierarchical_on_variables';
  HIERARCHICAL_ON_VD  : 'hierarchical_on_valuedomains';
  HIERARCHY           : 'hierarchy';
  IF                  : 'if';
  IMBALANCE           : 'imbalance';
  IN                  : 'in';
  INDEXOF             : 'indexof';
  INNER_JOIN          : 'inner_join';
  INPUT               : 'input';
  INSTR               : 'instr';
  INTDAY              : 'intday';
  INTEGER             : 'integer';
  INTERSECT           : 'intersect';
  INTMONTH            : 'intmonth';
  INTYEAR             : 'intyear';
  INVALID             : 'invalid';
  IS                  : 'is';
  ISNULL              : 'isnull';
  KEEP                : 'keep';
  KEY                 : 'key';
  KEYS                : 'keys';
  LAG                 : 'lag';
  LANGUAGE            : 'language';
  LAST                : 'last';
  LAST_VALUE          : 'last_value';
  LCASE               : 'lower';
  LEAD                : 'lead';
  LEFT_JOIN           : 'left_join';
  LEN                 : 'length';
  LIST                : 'list';
  LN                  : 'ln';
  LOG                 : 'log';
  LTRIM               : 'ltrim';
  MAP_FROM            : 'map_from';
  MAP_TO              : 'map_to';
  MAPS_FROM           : 'maps_from';
  MAPS_TO             : 'maps_to';
  MAX                 : 'max';
  MAX_LENGTH          : 'maxLength';
  MEASURE             : 'measure';
  MEASURES            : 'measures';
  MEDIAN              : 'median';
  MERGE               : 'merge';
  MIN                 : 'min';
  MM                  : 'mm';
  MOD                 : 'mod';
  MONTH_OP            : 'getmonth';
  MONTHTODAY          : 'monthtoday';
  NO_MEASURES         : 'no_measures';
  NON_NULL            : 'non_null';
  NON_ZERO            : 'non_zero';
  NOT                 : 'not';
  NOT_IN              : 'not_in';
  NULL_CONSTANT       : 'null';
  NUMBER              : 'number';
  NVL                 : 'nvl';
  ON                  : 'on';
  OPERATOR            : 'operator';
  OR                  : 'or';
  ORDER               : 'order';
  OUTPUT              : 'output';
  OVER                : 'over';
  PARTIAL             : 'partial';
  PARTIAL_NULL        : 'partial_null';
  PARTIAL_ZERO        : 'partial_zero';
  PARTITION           : 'partition';
  PERIOD_INDICATOR    : 'period_indicator';
  PIVOT               : 'pivot';
  POINT               : 'point';
  POINTS              : 'points';
  POWER               : 'power';
  PRECEDING           : 'preceding';
  RANDOM              : 'random';
  RANGE               : 'range';
  RANK                : 'rank';
  RATIO_TO_REPORT     : 'ratio_to_report';
  RECORD              : 'record';
  REGEXP              : 'regexp';
  RENAME              : 'rename';
  REPLACE             : 'replace';
  RESTRICT            : 'restrict';
  RETURN              : 'return';
  RETURNS             : 'returns';
  ROLE                : 'componentRole';
  ROUND               : 'round';
  ROWS                : 'rows';
  RTRIM               : 'rtrim';
  RULE                : 'rule';
  RULE_PRIORITY       : 'rule_priority';
  RULESET             : 'ruleset';
  SCALAR              : 'scalar';
  SET                 : 'set';
  SETDIFF             : 'setdiff';
  SINGLE              : 'single';
  SQRT                : 'sqrt';
  STDDEV_POP          : 'stddev_pop';
  STDDEV_SAMP         : 'stddev_samp';
  STOCK_TO_FLOW       : 'stock_to_flow';
  STRING              : 'string';
  STRUCTURE           : 'structure';
  SUBSPACE            : 'sub';
  SUBSTR              : 'substr';
  SUM                 : 'sum';
  SYMDIFF             : 'symdiff';
  THEN                : 'then';
  TIME                : 'time';
  TIME_AGG            : 'time_agg';
  TIME_PERIOD         : 'time_period';
  TIMESHIFT           : 'timeshift';
  TO                  : 'to';
  TOTAL               : 'total';
  TRIM                : 'trim';
  TRUNC               : 'trunc';
  TYPE                : 'type';
  UCASE               : 'upper';
  UNBOUNDED           : 'unbounded';
  UNION               : 'union';
  UNIT                : 'unit';
  UNPIVOT             : 'unpivot';
  USING               : 'using';
  VALID               : 'valid';
  VALUE               : 'Value';
  VALUE_DOMAIN        : 'valuedomain';
  VALUEDOMAINS        : 'valuedomains';
  VAR_POP             : 'var_pop';
  VAR_SAMP            : 'var_samp';
  VARIABLE            : 'variable';
  VARIABLES           : 'variables';
  VIRAL               : 'viral';
  WHEN                : 'when';
  WITH                : 'with';
  XOR                 : 'xor';
  YEAR_OP             : 'getyear';
  YEARTODAY           : 'yeartoday';
  YYYY                : 'yyyy';

fragment
DIGITS0_9
  : '0'..'9'
  ;

INTEGER_CONSTANT
  : DIGITS0_9+
  ;

NUMBER_CONSTANT
  : INTEGER_CONSTANT '.' INTEGER_CONSTANT
  ;

BOOLEAN_CONSTANT
  : 'true'
  | 'false'
  ;

STRING_CONSTANT
  : '"' (~'"')* '"'
  ;

fragment
ID_PART
  : [A-Za-z_] [A-Za-z0-9_.]*
  ;
  
IDENTIFIER
  : (ID_PART ':')? ID_PART ( '(' [0-9._+*~]+ ')' )? ( '/' ('.' | ID_PART)+ )?
  | '\'' ( '\\\'' | [^'] )* '\''
  ;
  
WS
  : [ \t\r\n\u000C]+ ->channel(1)
  ;

EOL
  : ';'
  ;

ML_COMMENT
  : '/*' .*? '*/' -> channel(2)
  ;

SL_COMMENT
  : '//' ~[\r\n]*  ->channel(2)
  ;
