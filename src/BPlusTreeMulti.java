import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BPlusTreeMulti extends BPlusTree {

    public Lock rootLock = new ReentrantLock();

    BPlusTreeMulti(String file, int degree) throws NumberFormatException, Exception {
        super(file, degree);
    }

    // public void delete_list_Multi(ArrayList<Data> listData) throws Exception
    // {
    // rootLock.lock();
    // try {
    // for( Data d : listData ){
    // super.delete(d.idx);
    // }
    // }
    // finally{
    // rootLock.unlock();
    // }
    // }
    // public void delete_multi(int key) throws Exception {
    // rootLock.lock();
    // try {
    // boolean in = false;
    // LeafNode toDelete = this.findLeafNode_multi(key, false);
    // for (int i = 0; i < toDelete.keys.size(); i++) {
    // if (toDelete.keys.get(i) == key) {
    // in = true;
    // break;
    // }
    // }
    // if (!in) {
    // throw new Exception("Key " + key + " is not in the tree so cannot be
    // deleted");
    // }

    // toDelete.delete(key);
    // } finally {
    // rootLock.unlock();
    // }

    // }

    @Override
    public Data search(int key) {

        LeafNode leafNode = findLeafNode_multi(key, true);
        // leafNode.lock = true;
        for (int i = 0; i < leafNode.keys.size(); i++) {
            if (leafNode.keys.get(i) == key) {
                // leafNode.lock2.unlock();
                leafNode.readLock.unlock();
                return leafNode.list_data.get(i);
            }
        }
        leafNode.readLock.unlock();
        // leafNode.lock2.unlock(); //600%0 mieux pas mielleur
        System.out.println("Key " + key + " is not in the tree, but could be inserted in this leaf : " + leafNode.keys);
        return null;
        // throw new Exception("Key " + key + " is not in the tree, but could be
        // inserted in this leaf : "+leafNode.keys);
    }

    public LeafNode findLeafNode_multi(int key, boolean ifsearch) {

        if (ifsearch) {

            root.readLock.lock();
            // root.lock2.lock();
        }
        int index = root.keys.size();
        for (int i = 0; i < root.keys.size(); i++) {
            if (key < root.keys.get(i)) {
                index = i;
                break;
            }
        }

        Node child = root.children.get(index);
        if (ifsearch) {
            child.readLock.lock();
            root.readLock.unlock();
        }
        if (child instanceof LeafNode) {
            return (LeafNode) child;
        } else {
            return findLeafNode((InnerNode) child, key, ifsearch);
        }
    }

    private LeafNode findLeafNode(InnerNode node, int key, boolean ifsearch) {

        int i;
        // Find next node on path to appropriate leaf node
        for (i = 0; i < node.keys.size(); i++) {
            if (key < node.keys.get(i)) {
                break;
            }
        }
        // return child if until child found is leafnode

        Node child = node.children.get(i);
        if (ifsearch) {
            child.readLock.lock();
            node.readLock.unlock();
        }
        if (child instanceof LeafNode) {
            return (LeafNode) child;
        } else {
            return findLeafNode((InnerNode) child, key, ifsearch);
        }
    }

}
