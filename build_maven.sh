#MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=256m" build/mvn -Dhadoop.version=2.2.0 -Phive -Phive-0.13.1 -Phive-thriftserver -DskipTests -Dscalastyle.failOnViolation=false clean
MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512m -XX:ReservedCodeCacheSize=512m" build/mvn -Dhadoop.version=2.2.0 -Phive -Phive-0.13.1 -Phive-thriftserver -DskipTests -Dscalastyle.failOnViolation=false clean
MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512m -XX:ReservedCodeCacheSize=512m" build/mvn -Dhadoop.version=2.2.0 -Phive -Phive-0.13.1 -Phive-thriftserver -DskipTests -Dscalastyle.failOnViolation=false package
#MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=256m" build/mvn -Dhadoop.version=2.2.0 -Phive -Phive-0.13.1 -Phive-thriftserver -DskipTests -Dscalastyle.failOnViolation=false install
#sbt/sbt -Dhadoop.version=2.2.0 -Phive -Phive-0.13.1 -Phive-thriftserver gen-idea
#./make-distribution.sh --tgz -Dhadoop.version=2.2.0 -Phive -Phive-0.13.1 -Phive-thriftserver
