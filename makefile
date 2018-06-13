min:
	javac -cp `hadoop classpath` *.java
	jar cvf MST.jar *.class
	hadoop fs -put ./input ~/
	mkdir -p ./output_min
	hadoop jar MST.jar MST ~/input/ ~/output_min/ min
	hadoop fs -cat ~/output_min/part* > ./output_min/resultingEdges.txt
	#cp ./stuff/cycle.py ./output_min/

max:
	javac -cp `hadoop classpath` *.java
	jar cvf MST.jar *.class
	hadoop fs -put ./input ~/
	mkdir -p ./output_max
	hadoop jar MST.jar MST ~/input/ ~/output_max/ max
	hadoop fs -cat ~/output_max/part* > ./output_max/resultingEdges.txt
	#cp ./stuff/cycle.py ./output_max/

rmin:
	rm *.class
	rm *.jar
	rm -r output_min/

rmax:
	rm *.class
	rm *.jar
	rm -r output_max/

clean:
	hadoop fs -rm -r ~/input ~/output_min/ ~/output_max/
	rm -r *.class *.jar output_max/ output_min/
