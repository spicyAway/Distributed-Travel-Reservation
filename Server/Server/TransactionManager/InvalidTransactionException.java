package Server.TransactionManager;

public class InvalidTransactionException extends Exception
{

    private int m_xid = 0;

    public InvalidTransactionException(int xid)
    {
        super("The transaction " + xid + " is Invalid.");
        m_xid = xid;
    }

    public int getXId()
    {
        return m_xid;
    }

}