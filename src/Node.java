import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Map;
import java.util.Set;

public abstract class Node {

    ArrayList<Integer> keys = new ArrayList<Integer>();
    Node parent = null;

    abstract Data getData(Integer key);

    abstract void deleteData(Integer key);

    abstract boolean isOverflow();

    abstract Node split() throws Exception;
}