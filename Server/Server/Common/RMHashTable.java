package Server.Common;

import java.util.*;

// A specialization of HashMap with some extra diagnostics
public class RMHashTable extends Hashtable<String, RMItem>
{
    public RMHashTable() {
        super();
    }

    public String toString()
    {
        String s = "--- BEGIN RMHashTable ---\n";
        for (String key : keySet())
        {
            String value = get(key).toString();
            s = s + "[KEY='" + key + "']" + value + "\n";
        }
        s = s + "--- END RMHashTable ---";
        return s;
    }

    public void dump()
    {
        System.out.println(toString());
    }

    public Object clone()
    {
        RMHashTable obj = new RMHashTable();
        for (String key : keySet())
        {
            obj.put(key, (RMItem)get(key).clone());
        }
        return obj;
    }
}