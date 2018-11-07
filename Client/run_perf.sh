# Usage: ./run_client.sh [<server_hostname> [<server_rmiobject>]]

java -Djava.security.policy=java.policy -cp ../Server/RMIInterface.jar:../Server/TException.jar:. Client.PerfClient $1 $2 $3 $4 $5 $6
