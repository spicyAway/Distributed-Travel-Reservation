all: java.policy compile-client

java.policy: FORCE
	@echo "Creating client java policy"
	@echo "grant codebase \"file:`pwd`/\" {" > java.policy
	@echo "permission java.security.AllPermission;" >> java.policy
	@echo "};" >> java.policy

FORCE: ;

../Server/RMIInterface.jar: ../Server/Server/Interface/IResourceManager.java
	make -C ../Server/ RMIInterface.jar

../Server/TException.jar: ../Server/Server/TransactionManager/*.java
	make -C ../Server/ TException.jar

# ../Server/SMessage.jar: ../Server/Server/TCP/SMessage.java
# 	make -C ../Server/RMIInterface.jar SMessage.jar

compile-client: ../Server/RMIInterface.jar ../Server/TException.jar
	javac -cp ../Server/TException.jar:../Server/RMIInterface.jar Client/*.java

# compile-client: ../Server/ SMessage.jar
# 	javac -cp ../Server/ SMessage.jar Client/*.java

clean:
	rm -f Client/*.class
	rm -f ../Server/RMIInterface.jar
	rm -f ../Server/SMessage.jar
	rm -f java.policy
