package Server.TransactionManager;

public class TransactionAbortedException extends Exception
{
    private int m_xid = 0;

    public TransactionAbortedException(int xid)
    {
        super("The transaction " + xid + " is Aborted.");
        m_xid = xid;
    }

    public int getXId()
    {
        return m_xid;
    }
}