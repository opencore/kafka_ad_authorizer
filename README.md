# Custom Kafka Authorizer to retrieve user roles from Active Directory #

This repository contains the source code that accompaniest a [blog post](http://testwww.opencore.com/blog/2018/3/2018-group-based-authorization-in-kafka/) on the OpenCore homepage.

### How do I get set up? ###

Pretty much just clone the repository, you'll also need a JDK and Maven.

This code is only useful in combination with a security enabled Kafka cluster.

### Deployment ###
After building upload target/adgroupauthorizer-1.0-SNAPSHOT-jar-with-dependencies.jar to all machines on your (security enabled) cluster and add the following options to your broker configuration:

```properties
authorizer.class.name=com.opencore.kafka.ComplexAclAuthorizer
principal.builder.class=com.opencore.kafka.HadoopGroupMappingPrincipalBuilder
```
and add the directory containing your jar to your CLASSPATH before starting your kafka  brokers.

```bash
export CLASSPATH=/path/to/your/file.jar
./kafka-server-start.sh ../config/kafka.properties
```


### Who do I talk to? ###

* Current lead: [soenkeliebau](http://github.com/soenkeliebau)