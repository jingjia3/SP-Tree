# SP-Tree

SP-Tree is a SPARQL query processor for Hadoop based on Spark GraphX, using the Pregel model, to answer subgraph matching queries on big RDF graphs. In our method, the query graph is transformed to a variant spanning tree based on the shortest paths.

## 1. Development environment configuration and version.

| Software                     | Version                                 |
|------------------------------| --------------------------------------- |
| Spark                        |  2.2.0                                  |
| Hadoop                       |  2.7.4                                  |
| Scala                        |  2.11.8                                 |
| JDK                          |  1.8                                    |

## 2. The example SPARQL query
```
PREFIX gn: <http://www.geonames.org/ontology#>
PREFIX gr: <http://purl.org/goodrelations/>
PREFIX mo: <http://purl.org/ontology/mo/>
PREFIX og: <http://ogp.me/ns#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX sorg: <http://schema.org/>
PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
PREFIX rev: <http://purl.org/stuff/rev#>
SELECT ?v0 ?v4 ?v6 ?v7 WHERE {	
        ?v0     sorg:caption    ?v1 .
        ?v0     sorg:text       ?v2 .
        ?v0     sorg:contentRating      ?v3 .
        ?v0     rev:hasReview   ?v4 .
        ?v4     rev:title       ?v5 .
        ?v4     rev:reviewer    ?v6 .
        ?v7     sorg:actor      ?v6 .
        ?v7     sorg:language   ?v8 .
}
```

## 3. The example submit command

### 3.1 Spark cluster
```
spark-submit --class gx.pl.sparql.QueryExecutor --master spark://node01:7077 Optimization-1.0-jar-with-dependencies.jar watdiv/watdiv1M.nt watdivQuery/10/C3.rq
```

### 3.2 Yarn Client
```
spark-submit --class gx.pl.sparql.QueryExecutor --master yarn --deploy-mode client Optimization-1.0-jar-with-dependencies.jar watdiv/watdiv1M.nt watdivQuery/10/C3.rq
```