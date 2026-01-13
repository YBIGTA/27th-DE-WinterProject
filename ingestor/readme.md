## compile
javac test_ingestion.java

## run
java test_ingestion

## preliminary check
curl -X POST http://localhost:8080/ingest -d "hello"
