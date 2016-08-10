# main file
main=WordCount
# jar file
jar=wc
# input file
input=/datasets/compleat.txt
# output folder
output=output
reduce_task_num=2
me = $(shell whoami)

all: compile

compile: ${main}.java
	hadoop com.sun.tools.javac.Main ${main}.java
	jar cf ${jar}.jar ${main}*.class

run: ${jar}.jar
	hadoop jar ${jar}.jar ${main} ${input} ${output} -D mapred.reduce.tasks=${reduce_task_num}

result:
	hdfs dfs -cat /user/${me}/${output}/* | less

clean_output:
	hdfs dfs -rm -r /user/${me}/${output}

clean_local:
	rm ${jar}.jar
	rm ${main}*.class


	
