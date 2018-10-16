# splodge
Reimplementation of SPLODGE [1] to scale up to graphs with several billion edges.

[1] Görlitz, O., Thimm, M., & Staab, S. (2012). Splodge: Systematic generation of sparql benchmark queries for linked open data. The Semantic Web–ISWC 2012, 116–132. Retrieved from http://link.springer.com/chapter/10.1007/978-3-642-35176-1_8

## Building
`mvn clean package`

## Usage
If you call SPLODGE the first time, you need the parameter -i that points to the input graph file (or a directory that contains several files that should be loaded as a single graph). With the parameter -w you specify a folder in which the statistics of the graph is created. After the statistics have been created during the first call, -i can be skipped for any further call since the statistics specified by -w will be sufficient. If the -w is not specified, the temp folder is used by default. If the statistical data should be automatically deleted after the generation of the query, -r can be added.

With the parameter -o the directory in which the SPARQL queries should be created is defined. Each SPARQL query will be stored in a separate file. With the parameter -l <value> a LIMIT <value> statement is added to the end of the generated SPARQL queries.

The characteristics of the generated queries can be described by the following parameters:
* -j specifies the type of join patterns. Supported values are either SUBJECT_SUBJECT_JOIN for star-shaped queries or SUBJECT_OBJECT_JOIN for path shaped queries.
* -n specifies the number of joins that should be performed. E.g., -j SUBJECT_OBJECT_JOIN -n 3 generates a path shaped query consisting of 4 triple patterns.
* -d specifies the number of data sources (i.e., top level domains) that should be used
* -s specifies the minimal selectivity of the query (i.e., sum of the selectivities of all triple patterns). E.g. the selectivity -s 0.1 will generate a query with a selectivity [0.1, 1]
* -m specifies the maximal selectivity of the query (i.e., sum of the selectivities of all triple patterns). It should be used in conjunction with -s. E.g., -s 0.1 -m 0.2 will generate a query with selectivity [0.1, 0.2] whereas -s 0.1 -m 0.1 will generate a query with the selectivity of exact 0.1.

Since the generation of the queries will be performed using a depth first search starting with properties that have the highest selectivity, it can take a long time to find a basic graph pattern that matches the specified requirements. To speed up the search, the option -t can be used. It specifies a timeout in msec after which the search should start with the succeeding property.

Example usage: ` java -jar target/splodge-complete.jar -i 01data-3-00.nq.gz -o /tmp -j SUBJECT_OBJECT_JOIN -n 2 -d 3 -s 0.01  -m 0.02 -t 500 -l 1000000`