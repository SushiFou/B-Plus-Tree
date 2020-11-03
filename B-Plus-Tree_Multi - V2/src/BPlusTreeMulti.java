import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class BPlusTreeMulti extends BPlusTree {
    public Lock rootLock= new ReentrantLock();



    BPlusTreeMulti(String file, int degree) throws NumberFormatException, Exception {
        super(file, degree);
    }


    public void add_file_Multi(ArrayList<Data> listData) throws Exception
    {
        rootLock.lock();
        try {
            for( Data d : listData ){
                super.add(d.idx, d);
            }
        } 
        finally{
            rootLock.unlock();
        }
    }
    public void add_file(ArrayList<Data> listData) throws Exception
    {
        for( Data d : listData ){
                super.add(d.idx, d);
        }
 
    }

    public void createLeavesLayer(ArrayList<Data> listData, Float fillingFactor) throws Exception {


            LeafNode actualLeafNode = this.firstLeafNode;
            int num_keys = (int) (fillingFactor * (this.degree - 1));
            int i = 0;

            for(Data d : listData) {

                    if (i < num_keys) {
                        actualLeafNode.addData(d.idx, d);
                        i++;
                    } 
                    else {
                        // key limit so we create a new leaf and we make them siblings
                        i = 0;
                        BPlusTree.LeafNode newLeaf = new LeafNode();
                        newLeaf.previous = actualLeafNode; // make siblings
                        actualLeafNode.next = newLeaf;
                        actualLeafNode = newLeaf;
                        actualLeafNode.addData(d.idx, d);
                        i++;
                    }
            }

    }

    // need the csv to be sorted (ExternalMergeSort)
    // the filling factor is a number between 0.5 and 1 -> % of how much we fill the
    // leaves
    public void bulk_loading(ArrayList<Data> listData, Float fillingFactor) throws NumberFormatException, Exception {
        rootLock.lock();
        createLeavesLayer(listData, fillingFactor);

        BPlusTree.LeafNode actual = this.firstLeafNode;

        while (actual != null) {
            // build tree upon the leaves
            if (actual != this.firstLeafNode) {
                if (actual.previous.parent != null) {
                    // System.out.println("parent is not null");
                    actual.previous.parent.addKey(actual.keys.get(0), actual);
                    if (actual.previous.parent.isOverflow()) {

                        // System.out.println("parent is Overflow");
                        Node p = actual.previous.parent;
                        // repeat until no split is possible
                        while (p != null) {
                            if (p.isOverflow()) {
                                p = p.split(); // return parent
                            } else {
                                break;
                            }
                        }
                    }
                } else {
                    InnerNode newParent = new InnerNode();
                    newParent.keys.add(actual.keys.get(0));
                    newParent.children.add(actual.previous);
                    newParent.children.add(actual);
                    actual.previous.parent = newParent;
                    actual.parent = newParent;
                    this.root = newParent;
                }

            }
            actual = actual.next;
        }
        rootLock.unlock();

    }
    public void delete_list_Multi(ArrayList<Data> listData) throws Exception
    {
        rootLock.lock();
        try {
            for( Data d : listData ){
                super.delete(d.idx);
            }
        } 
        finally{
            rootLock.unlock();
        }
    }
    public void delete_multi(int key) throws Exception {
        rootLock.lock();
        try {
            boolean in = false;
            LeafNode toDelete = this.findLeafNode_multi(key, false);
            for (int i = 0; i < toDelete.keys.size(); i++) {
                if (toDelete.keys.get(i) == key) {
                    in = true;
                    break;
                }
            }
            if (!in) {
                throw new Exception("Key " + key + " is not in the tree so cannot be deleted");
            }
    
            toDelete.delete(key);
        } finally {
            rootLock.unlock();
        }


    }

    @Override
    public Data search(int key) throws Exception {
        
        LeafNode leafNode= findLeafNode_multi(key, true);
        //leafNode.lock = true;
        for (int i = 0; i < leafNode.keys.size(); i++){
            if (leafNode.keys.get(i)==key){
                //leafNode.lock2.unlock();
                leafNode.lock.readLock().unlock();
                return leafNode.list_data.get(i);
            }
        }
        leafNode.lock.readLock().unlock();
        //leafNode.lock2.unlock(); //600%0 mieux pas mielleur
        throw new Exception("Key " + key + " is not in the tree, but could be inserted in this leaf : "+leafNode.keys);
    }
    
    public LeafNode findLeafNode_multi(int key, boolean ifsearch) {

        if( ifsearch) {
            root.lock.readLock().lock();
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
            child.lock.readLock().lock();
            root.lock.readLock().unlock();
            
            //  child.lock2.lock();
            //  root.lock2.unlock();
            
        }
        if (child instanceof LeafNode) {
            return (LeafNode) child;
        } else {
            return findLeafNode((InnerNode) child, key,ifsearch);
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
            child.lock.readLock().lock();
            node.lock.readLock().unlock();

            //  child.lock2.lock();
            //  node.lock2.unlock();
            
        }
        if (child instanceof LeafNode) {
            return (LeafNode) child;
        } else {
            return findLeafNode((InnerNode) child, key, ifsearch);
        }
    }

   

}
