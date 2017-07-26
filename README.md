# Neo4j Versioner SQL

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

It's based on the following data model: 

![Data Model](https://raw.githubusercontent.com/h-omer/neo4j-versioner-sql/master/docs/images/data-model.png)
