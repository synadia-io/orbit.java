call gradlew clean build jacocoTestReport
taskkill /F /IM nats-server.exe
start chrome file:///C:/nats/orbit.java/counter/build/reports/jacoco/test/html/index.html
start chrome file:///C:/nats/orbit.java/counter/build/reports/tests/test/index.html

