****Commands for db2****

To start db2 -
db2start 

To connect to database -
db2 connect to SAMPLE

To run sql scripts -
db2 -td@ -f stddev.sql


****Commands for Java****

Assuming database is running on - localhost:50000

To export db2jcc4.jar -
export CLASSPATH=.:/Users/chumkiacharya/Documents/db2jcc-db2jcc4.jar

To compile java file -
javac SalaryStdDev.java

To run java file -
java SalaryStdDev dbname tablename userid password 