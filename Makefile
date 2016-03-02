uberjar:
		./gradlew shadowJar

test: uberjar
		./gradlew test --info

run: uberjar
	java -jar build/libs/sstable-kt-all.jar
