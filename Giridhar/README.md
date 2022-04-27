#### This project is to find the page rank for web04 folder.
#### Check your java version using:
```
java --version
```

#### Generate a maven project by using the following command:
```
mvn archetype:generate 
-D archetypeGroupId=org.apache.beam
-D archetypeArtifactId=beam-sdks-java-maven-archetypes-examples
-D archetypeVersion=2.36.0
-D groupId=org.example
-D artifactId=word-count-beam
-D version="0.1"
-D package=org.apache.beam.examples
-D interactiveMode=false
```

#### Run the minimal page rank with the following command:

```
mvn compile exec:java -D exec.mainClass=org.apache.beam.giridhar.MinimalPageRankAddagalla

```




[Personal Wiki](https://github.com/vyshnavi1996/Beam-Dataproc-Java/wiki/Giridhar-Addagalla)
