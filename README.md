# jdbcq

JDCBQ is a command-line (cli) Java based JDBC query and performance measurement tool.

It initially grew out of a need to evaluate different versions/vendors JDBC drivers but like any handy piece of software it's grown to inclue meta-data interrogation, column filting, and some driver specific features such as streaming.

## Quickstart
    
```cmd
   $ gradle fatjar
   $ java -jar build/libs/jdbcq-all-0.1.2.jar -d jdbc:mysql://localhost:3306/employees -u user -p user -i
   MySQL Connector Java/mysql-connector-java-5.1.35 ( Revision: 5fb9c5849535c13917c2cf9baaece6ef9693ef27 ) -> MySQL/5.6.14
```

Once in interactive mode (-i on the command line) you can access the DB tables along with the metadata.
```cmd
   </>$ gradle fatjar
   </>$ java -jar build/libs/jdbcq-all-0.1.2.jar -d jdbc:mysql://localhost:3306/employees -u user -p user -i
   MySQL Connector Java/mysql-connector-java-5.1.35 ( Revision: 5fb9c5849535c13917c2cf9baaece6ef9693ef27 ) -> MySQL/5.6.14
   <jdbcq> show catalogs
   TABLE_CAT
   information_schema
   employees
   <jdbcq> show tables employees
   TABLE_CAT	TABLE_SCHEM	TABLE_NAME  	TABLE_TYPE	REMARKS	TYPE_CAT	TYPE_SCHEM	TYPE_NAME	SELF_REFERENCING_COL_NAME	REF_GENERATION
   employees	           	departments 	TABLE
   employees	           	dept_emp    	TABLE
   employees	           	dept_manager	TABLE
   employees	           	employees   	TABLE
   employees	           	salaries    	TABLE
   employees	           	titles      	TABLE
   <jdbcq> use employees
   use is employees.
   <jdbcq> show table titles
   TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	NUM_PREC_RADIX	NULLABLE	REMARKS	COLUMN_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE	SCOPE_CATALOG	SCOPE_SCHEMA	SCOPE_TABLE	SOURCE_DATA_TYPE	IS_AUTOINCREMENT	IS_GENERATEDCOLUMN
   employees	           	titles    	emp_no     	4        	INT      	10         	65535        	0             	10            	0       	       	          	0            	0               	                 	1               	NO         	             	            	           	                	NO
   employees	           	titles    	title      	12       	VARCHAR  	50         	65535        	              	10            	0       	       	          	0            	0               	50               	2               	NO         	             	            	           	                	NO
   employees	           	titles    	from_date  	91       	DATE     	10         	65535        	              	10            	0       	       	          	0            	0               	                 	3               	NO         	             	            	           	                	NO
   employees	           	titles    	to_date    	91       	DATE     	10         	65535        	              	10            	1       	       	          	0            	0               	                 	4               	YES        	             	            	           	                	NO
   keys (primary) emp_no,title,from_date (export)  (import) employees.emp_no->emp_no
   indexes emp_no[primary:other:asc:1:442426:0],title[primary:other:asc:2:442426:0],from_date[primary:other:asc:3:442426:0],emp_no[emp_no:other:asc:1:442426:0]
   <jdbcq> display titles
   emp_no	title          	from_date 	to_date
   10001 	Senior Engineer	1986-06-26	9999-01-01
   10002 	Staff          	1996-08-03	9999-01-01
   10003 	Senior Engineer	1995-12-03	9999-01-01
   10004 	Engineer       	1986-12-01	1995-12-01
   10004 	Senior Engineer	1995-12-01	9999-01-01
   10005 	Senior Staff   	1996-09-12	9999-01-01
   10005 	Staff          	1989-09-12	1996-09-12
   10006 	Senior Engineer	1990-08-05	9999-01-01
   10007 	Senior Staff   	1996-02-11	9999-01-01
   10007 	Staff          	1989-02-10	1996-02-11
   <jdbcq> display titles emp_no 10 12
   emp_no	title             	from_date 	to_date
   10007 	Staff             	1989-02-10	1996-02-11
   10008 	Assistant Engineer	1998-03-11	2000-07-31
   <jdbcq> select emp_no,title from titles limit 3
   emp_no	title
   10001 	Senior Engineer
   10002 	Staff
   10003 	Senior Engineer
   <jdbcq> create table awesome (d date)
   update count is 0
   <jdbcq> describe awesome
   COLUMN_NAME	COLUMN_TYPE	IS_NULLABLE	COLUMN_KEY	COLUMN_DEFAULT	EXTRA
   d          	date       	YES
   <jdbcq> drop table awesome
   update count is 0
   <jdbcq> show grants for user
   Grants for user@%
   GRANT USAGE ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD '*D5D9F81F5542DE067FFF5FF7A4CA4BDD322C578F'
   GRANT ALL PRIVILEGES ON `employees`.* TO 'user'@'%'
```
## Introduction

JDBCQ contains its own set of commands (type 'help') which provide a consistent interface across all DBs, any
commands that JDBCQ does not know are passed straight through as sql statements (e.g. show grants) so you have
direct access to the SQL engine of the DB.

To connect to other databases simply modify the -d (database url) option to point to some other DB (see below). Also
you can run JDBCQ in a non-interactive mode like the examples below.

    $ java -jar build/libs/jdbcq-all-0.1.2.jar -d jdbc:postgresql://localhost/ -u postgres -m
    table_schem table_catalog
    information_schema  null
    pg_catalog  null
    $ java -jar build/libs/jdbcq-all-0.1.2.jar -d jdbc:postgresql://localhost/ -u postgres -m pg_catalog
    ...

The -w option can be used to force all schema/table names into uppercase, which is sometimes helpful
if you're not seeing any output (ala oracle).

### jdbcq.conf

An optional configuration file can be placed in your home directory (~/.jdbcq.conf) filled with the database sources that you often use.  The format is a yaml document rooted at 'sources:' followed by source name containing url, user, password.  For example:

    sources:
      emp:
        url: jdbc:mysql://localhost/employees
        user: user
        password: user
      ora:
        url: jdbc:oracle:thin:@oradb1:1500:test
        user: test
        password: test

The driver is auto-selected based on the contents of the url.  That is, for emp, the string 'mysql' will trigger a binding to 'com.mysql.jdbc.Driver' or you can explicitly define it with an additional line like: 'driver: org.postgresql.Driver'

All of these paramters can be overriden on the command line; --db for connection url, --user and --password, while --jdbc can be used to identify a specific driver (make sure the driver jar is on the classpath or you can embed it directly into jdbcq.jar)

## Custom Drivers

To include a custom JDBC driver one option is to manually download the jar for example ojdbc6.jar from:

     http://www.oracle.com/technetwork/database/enterprise-edition/jdbc-112010-090769.html

and then use your central maven repository to house it like so:

     mvn install:install-file -Dfile=ojdbc6.jar -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=11.2.0.4 -Dpackaging=jar -DgeneratePom=true

then add a line to the dependencies list in gradle.build using groupId:artifactId:version that you specified with the maven install command:

    compile('com.oracle:ojdbc6:11.2.0.4')

Now you should see your newly added jar in the list:

    $ gradle listjars
    :listjars
    snakeyaml-1.8.jar
    postgresql-9.4-1201-jdbc41.jar
    mysql-connector-java-5.1.6.jar
    ojdbc6-11.2.0.4.jar
    mongo-java-driver-3.0.0.jar
     
    BUILD SUCCESSFUL
     
    Total time: 5.169 secs

## Examples (non-interactive)

Assuming that 'jdbcq' is a stand-in for 'java -jar build/libs/jdbcq-all-0.1.2.jar', below are some examples of what jdbcq can do:

## Metadata

Shows a list of catalogs for the source identified by 'emp':

    $ jdbcq -s emp -m
    TABLE_CAT
    information_schema
    employees

Display table metadata:

    $ jdbcq -s emp -m employees
    TABLE_CAT TABLE_SCHEM TABLE_NAME  TABLE_TYPE  REMARKS
    employees null  departments TABLE
    employees null  dept_emp  TABLE
    ...

Column metadata can be had by adding the specific table name of interest, like so:

    $ jdbcq -s emp -m employees dept_emp
    TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	...
    employees	null	dept_emp	emp_no	4	INT	10	...
    employees	null	dept_emp	dept_no	1	CHAR	4	...
    ...
    keys (primary) emp_no,dept_no (export)  (import) departments.dept_no->dept_no
    indexes emp_no[primary:other:asc:1:331143:0],dept_no[primary:other:asc:2:331143:0],emp_no[emp_no:other:asc:1:331143:0],dept_no[dept_no:other:asc:1:16:0]

Notice above that additional information regarding primary, import and export keys is displayed showing the linkages.

Also the indexes are displayed showing the column, type (e.g. hash, cluster, other), ascending/descending, ordinal position, cardinality, and pages (see javadocs DatabaseMetaData.getIndexInfo() for details).

### Values

Table values can be obtained by simply specifying the table (without the -m option).  By default only 10 rows will be displayed.:

    $ jdbcq -s emp titles
    emp_no   title  from_date       to_date
    10001    Senior Engineer        1986-06-26      9999-01-01
    10002    Staff  1996-08-03      9999-01-01
    10003    Senior Engineer        1995-12-03      9999-01-01
    ...

A starting and ending row number can be used to select a subset of the rows to display (for large tables its better to use -q and write a custom query; see below).  You need to also identify a column (e.g emp_no) that the sort will be applied to in order to generate a stable row set.

    $ jdbcq -s emp titles emp_no 6 8
    emp_no   title      from_date   to_date
    10005    Senior Staff           1996-09-12      9999-01-01
    10005    Staff  1989-09-12      1996-09-12

### Custom SQL

The -q option lets you provide a SQL query to be expected.  Careful with this option as both input and output are not filtered.  All the results you receive will be displayed, also the contents of -q are sent directly to the database without modification.

    $ jdbcq -s emp -q "select emp_no,title from titles where emp_no<10003"
    emp_no   title
    10001    Senior Engineer
    10002    Staff

Statement.executeQuery() is used so all associated restrictions to the SQL delivered will apply.

### Timing

The -t option will output two times and no data.  The first time is how fast the database/driver responded to the query and returned with a ResultSet.  The 2nd time indicated how quickly the result set was parsed.  Each column specified is included as a string this computation, additionally truncation is applied if needed.  Allow the result set processing is not the most efficient, it does provide a consistent baseline that can be compared from driver to driver.  Watch out for db caching which will dramatically affect your results from run to run.

## Usage

Usage: jdbcq [-tn] [-c col_spec] [-q sql-query] table order-by [start [end]]

Executes a 'select *' from the specified table returning the first 10 rows. If provided,
'start' determines the starting row_number().  'end' can be used to indicate the terminating
row_number().

By default all columns are output unless the '-c' option is used.  Columns
are identified by their ordinal number and may be specified via a range (e.g. 5-8) or
or individually, separated by a comma (e.g. 11,5-8,2).

The '-t' is used to time the query and subsequent access of the results without displaying them.
The default query can be overridden with the '-q' option, in which case table,order-by,start
and end arguments are ignored.  Displaying only the meta-data (no results) associated with
the query is achieved by using the '-n' option.  Quoting order-by and appending DESC will result
in a descending sort order. E.g. jdbcq productsubmissions "submissionid DESC" -c 1,2