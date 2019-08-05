# SimpleSQL

SimpleSQL is a *very* simple Java library for fetching data from a database using JDBC.  The motivation for creating SimpleSQL was to be able to easily fetch data within a Groovy script in [Dell Boomi](https://boomi.com/) without having to deal with JDBC directly.  The standard Boomi [Database Connector](http://help.boomi.com/atomsphere/GUID-A0371711-C68D-4DE0-86E0-3AB54165DABE.html)) does not allow the SQL to be built dynamically.

## Usage

There are three variations on fetching data from the databse:

* `queryFirst` - fetching a single row
* `queryAsList` - fetching all rows in a single list
* `query` - fetching a row at a time and processing that row before moving on to the next.  Use this to have the most control over the fetching and processing of records.

All methods require, as their first parameter, a `ConnectionProperties` object that contains the settings required to be able to connect to the database.  There are two methods that will create such an object:

* `connectionParameters(driverClass, connectionString, user, password)`
* `connectionParametersFromFile(propertiesFile)`

The `connectionParametrsFromFile` reads a Java Properties file from the filesystem on which the Atom process is running.  The keys in the file are: driverClass, connectionString, user and password.  An example file is:

```
driverClass=oracle.jdbc.driver.OracleDriver
connectionString=jdbc:oracle:thin:@//mydbserver:1521/service_name
user=scott
password=tiger
```
Alternatively, you could incorporate reading an encrypted file (e.g. using [PGP](https://help.boomi.com/bundle/integration/page/c-atm-Certificate_components_9985dbf3-9b86-4983-a68d-53e7c6836763.html)) into your process and use the first method to supply the connection parameters.

### Fetching a Single Row

Use the `queryFirst` method.  The parameters are:

* connection properties
* the SQL string
* a list of parameters (if there are none then use `[]` or `null`)

A `Map` is returned containing the column values returned by the query.

```Groovy
import static com.boomi.execution.ExecutionUtil.*
import static groovy.json.JsonOutput.*
import static aberta.sql.SimpleSQL.*

def connectionProperties = ... 
def owner = ...
def queryParameters = ['UK' /* country code */, 'Y' /* active flag */]

def row = queryFirst(connectionProperties, """

    select CUSTOMER_ID
    from ${owner}.CUSTOMER_MASTER
    where COUNTRY = ? and ACTIVE = ?
    order by TOTAL_SALES desc

""", queryParameters)

if (row) { // output a single document as JSON
    dataContext.storeStream(
        new ByteArrayInputStream(toJson(row).getBytes()),
        new Properties())
}
```
In the example above the database record is output as a document for the next Shape to process.

### Fetching a List of Rows

If the number of rows returned by your query is expected to be relatively small then you
can call the `queryAsList`.  As you might expect the output is a `List`.  The following example is identical to the one for `queryFirst` except for the change in method.

```Groovy
import static com.boomi.execution.ExecutionUtil.*
import static groovy.json.JsonOutput.*
import static aberta.sql.SimpleSQL.*

def connectionProperties = ... 
def owner = ...
def queryParameters = ['UK' /* country code */, 'Y' /* active flag */]

def rows = queryAsList(connectionProperties, """

    select CUSTOMER_ID
    from ${owner}.CUSTOMER_MASTER
    where COUNTRY = ? and ACTIVE = ?
    order by TOTAL_SALES desc
    fetch first 10 rows only

""", queryParameters)

if (rows) { // output a single document as JSON
    dataContext.storeStream(
        new ByteArrayInputStream(toJson(rows).getBytes()),
        new Properties())
}
```
The example above outputs a JSON array of documents

### Processing Each Record

If you want more control over the processing of each record then use the `query` method.  The parameters are the same except that you specify a lambda function to process the record.  The function must return either `true` to continue fetching record or `false` to end the query.

```Groovy
import static com.boomi.execution.ExecutionUtil.*
import static groovy.json.JsonOutput.*
import static aberta.sql.SimpleSQL.*

def connectionProperties = ... 
def owner = ...
def queryParameters = ['UK' /* country code */]

def rows = query(connectionProperties, """

    select CUSTOMER_ID, ACTIVE
    from ${owner}.CUSTOMER_MASTER
    where COUNTRY = ?
    order by TOTAL_SALES desc
    fetch first 10 rows only

""", queryParameters) { row ->

    row.ACTIVE = (row.ACTIVE == 'Y')? "Active":"Not Active"

    dataContext.storeStream(
        new ByteArrayInputStream(toJson(row.getBytes()),
        new Properties())

    return true // return true to continue fetching, false to stop
}
```
The example above outputs one Document for each row returned.

### Mitigating against SQL-Injection Attacks

The library uses [`PreparedStatement`](https://en.wikipedia.org/wiki/Prepared_statement) to allow the use of `?` placeholders
for dynamic values (e.g. `where updateDate >= ?`).  This is a highly recommended 
practice to mitigate [SQL-Injection](https://en.wikipedia.org/wiki/SQL_injection) attacks. When using this library take *great care* to check any other value that is dynamically added to the SQL.  For example, if you are using this library to be able to dynamically
select a schema or table owner then ensure that the passed in values are from a list of
acceptable values.

## Installation

To be conveniently incorporated into a Boomi process SimpleSQL is written in [one source file](src/main/java/aberta/sql/SimpleSQL.java).  You can simple cut-and-paste the source code into a *Custom Scripting* step in a *Data Process Shape*.  

Alternatively you can install the `.jar` file into your Account and create a [Custom Library](https://help.boomi.com/bundle/integration/page/c-atm-Custom_Library_components_8844439e-657e-43eb-ab44-27568c52abed.html).
