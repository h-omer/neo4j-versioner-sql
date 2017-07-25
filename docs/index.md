# Neo4j Versioner SQL Documentation

Neo4j Versioner SQL is a collection of procedures, aimed to help developers to manage a SQL Database schema, through the Entity-State model applied to Neo4j.

## License

Apache License 2.0

## Installation

1. Neo4j Versioner Core is required, please see the current documentation [here](https://h-omer.github.io/neo4j-versioner-core/);
2. Download the latest [release](https://github.com/h-omer/neo4j-versioner-sql/releases);
3. Put the downloaded jar file into `$NEO4J_HOME/plugins` folder;
4. Put your database JDBC driver into `$NEO4J_HOME/plugins` folder, for more information on supported databases, click [here](currently-supported-databases). 
5. Start/Restart Neo4j.

## About

Neo4j Versioner SQL has been developed by [Alberto D'Este](https://github.com/albertodeste) and [Marco Falcier](https://github.com/mfalcier).

## Data Model

TODO INSERT DATA MODEL IMAGE AND STORY HERE :)

## Currently supported databases

* `postgres`
* `mysql`

# Procedures Reference

Neo4j procedure documentation can also be found using `CALL dbms.procedures()`.

## Procedure CheatSheet

Here you can find a "compressed" list of all the procedures:

Legend
* *Optional parameter*
* **Node/Path**

name | parameters | return values | description
---- | ---------- | ------------- | -----------
[sql.versioner.init](#init) | dbname, hostname, port, database, username, password | **node** | Initialize a Database Version with its schemas, tables and relative columns.
[sql.versioner.reload](#reload) | hostname, port, username, password | **node** | Reload a new Database Version with its schemas, tables and relative columns.

## init

This procedure is used in order to initialize a Database Version with its schemas, tables and relative columns.
...

### Details

#### Name

`sql.versioner.init`

#### Parameters

name | necessity | detail 
---- | --------- | ------
`dbname` | mandatory | The name of the database type you are going to import; see [supported databases](currently-supported-databases).
`hostname` | mandatory | The database hostname.
`port` | mandatory | The database port.
`database` | mandatory | The database name you are going to import.
`username` | mandatory | The username for connecting to the given database.
`password` | mandatory | The username's password.

#### Return value

name | type 
---- | ----
node | Node 

### Example call

```cypher
CALL sql.versioner.init('mysql', 'localhost', 3306, 'new_schema', 'root', 'password') YIELD node return node
```

## reload

This procedure is used in order to reload a new Database Version with its schemas, tables and relative columns.
...

### Details

#### Name

`graph.versioner.init`

#### Parameters

name | necessity | detail 
---- | --------- | ------
`hostname` | mandatory | The database hostname.
`port` | mandatory | The database port.
`username` | mandatory | The username for connecting to the given database.
`password` | mandatory | The username's password.

#### Return value

name | type 
---- | ----
node | Node 

### Example call

```cypher
CALL sql.versioner.reload('localhost', 3306, 'root', 'password') YIELD node return node
```