import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;

public class BPlusTree {

    // degree = Max branching factor (=max_children per node)
    // So Max keys = Max branching factor - 1

    int degree;
    InnerNode root = null;
    public LeafNode firstLeafNode;

    BPlusTree(String file, int degree) throws NumberFormatException, Exception {

        this.degree = degree;// test1
        this.firstLeafNode = new LeafNode();
        // add_file(file);
    }

    void print_Tree(Node node, int h) {
        if (node != null) {
            for (int key : node.keys) {
                for (int i = 0; i < h; i++) {
                    System.out.print("|---");
                }
                System.out.println(key);
            }
            if (!(node instanceof LeafNode)) {
                for (Node child : ((InnerNode) node).children) {
                    print_Tree(child, h + 1);
                    System.out.println("-----------------");
                }
            }
        }
    }

    void print_Tree() {
        System.out.println("B+Tree Printer :");
        if (this.root != null) {
            this.print_Tree(this.root, 0);
        }
        else{
            System.out.println(this.firstLeafNode.keys);
        }
    }

    // need the csv to be sorted (ExternalMergeSort)
    public void createLeavesLayer(String file, Float fillingFactor) throws NumberFormatException, Exception {

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            Boolean firstline = true;
            LeafNode actualLeafNode = this.firstLeafNode;
            int num_keys = (int) (fillingFactor * (this.degree - 1));
            int i = 0;

            while ((line = br.readLine()) != null) {

                String[] value = line.split(";");
                if (firstline) {
                    firstline = false;
                } else {
                    // create all leaves with fillingFactor*(degre-1) keys
                    if (i < num_keys) {
                        actualLeafNode.addData(Integer.parseInt(value[0]), new Data(value[1]));
                        i++;
                    } else {
                        // key limit so we create a new leaf and we make them siblings
                        i = 0;
                        BPlusTree.LeafNode newLeaf = new LeafNode();
                        newLeaf.previous = actualLeafNode; // make siblings
                        actualLeafNode.next = newLeaf;
                        actualLeafNode = newLeaf;
                        actualLeafNode.addData(Integer.parseInt(value[0]), new Data(value[1]));
                        i++;
                    }
                }
            }
        }
    }

    // need the csv to be sorted (ExternalMergeSort)
    // the filling factor is a number between 0.5 and 1 -> % of how much we fill the
    // leaves
    public void bulk_loading(String file, Float fillingFactor) throws NumberFormatException, Exception {
        createLeavesLayer(file, fillingFactor);

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

    }

    // read and add whole csv
    public void add_file(String file) throws NumberFormatException, Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            Boolean firstline = true;
            while ((line = br.readLine()) != null) {
                String[] value = line.split(";");
                if (firstline) {
                    firstline = false;
                } else {
                    add(Integer.parseInt(value[0]), new Data(value[1]));
                }
            }
        }
    }

    // add key value in tree, data per data
    public void add(Integer key, Data data) throws Exception {

        // find good leafNode
        // System.out.println("We want to add the key : " + key);

        LeafNode freeLeaf = (this.root == null) ? this.firstLeafNode : findLeafNode(key);
        // System.out.println("We found the leafNode :" + freeLeaf.keys);
        // if (freeLeaf.parent != null) {
        // System.out.println("With parents :" + freeLeaf.parent.keys);
        // }

        // add into
        freeLeaf.addData(key, data);
        // if the add generated an overflow
        if (freeLeaf.isOverflow()) {
            // System.out.println("LeafNode Overflow");
            Node broNode = freeLeaf.split();

            // split for the first time
            if (freeLeaf.parent == null) {
                // System.out.println("parent is null");
                // create parent for broNode and actual leafNode
                InnerNode newParent = new InnerNode();
                newParent.keys.add(broNode.keys.get(0));
                newParent.children.add(freeLeaf);
                newParent.children.add(broNode);
                freeLeaf.parent = newParent;
                broNode.parent = newParent;
                this.root = newParent;
            }

            // update parents
            else {
                // System.out.println("parent is not null");
                freeLeaf.parent.addKey(broNode.keys.get(0), broNode);
                if (freeLeaf.parent.isOverflow()) {

                    // System.out.println("parent is Overflow");
                    Node p = freeLeaf.parent;
                    // repeat until no split is possible
                    while (p != null) {
                        if (p.isOverflow()) {
                            p = p.split(); // return parent
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }

    public void delete(int key) throws Exception {

        boolean in = false;
        LeafNode toDelete = this.findLeafNode(key);
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
    }

    public Data search(int key) throws Exception {

        LeafNode leafNode = findLeafNode(key);

        for (int i = 0; i < leafNode.keys.size(); i++) {
            if (leafNode.keys.get(i) == key) {
                return leafNode.list_data.get(i);
            }
        }
        throw new Exception(
                "Key " + key + " is not in the tree, but could be inserted in this leaf : " + leafNode.keys);
    }

    private LeafNode findLeafNode(int key) {

        // Find next node on path to appropriate leaf node
        // if (key < root.keys.get(root.keys.size() - 1)) {
        if(this.root!=null){
            int index = root.keys.size();
            for (int i = 0; i < root.keys.size(); i++) {
                if (key < root.keys.get(i)) {
                    index = i;
                    break;
                }
            }
            // } else {
            // i = root.keys.size();
            // }
            // return child if until child found is leafnode
            Node child = root.children.get(index);
            if (child instanceof LeafNode) {
                return (LeafNode) child;
            } else {
                return findLeafNode((InnerNode) child, key);
            }
        }
        else {
            return this.firstLeafNode;
        }
    }

    private LeafNode findLeafNode(InnerNode node, int key) {

        int i;
        // Find next node on path to appropriate leaf node
        for (i = 0; i < node.keys.size(); i++) {
            if (key < node.keys.get(i)) {
                break;
            }
        }
        // return child if until child found is leafnode
        Node child = node.children.get(i);
        if (child instanceof LeafNode) {
            return (LeafNode) child;
        } else {
            return findLeafNode((InnerNode) child, key);
        }
    }

    class LeafNode extends Node {

        ArrayList<Data> list_data = new ArrayList<Data>();
        LeafNode next = null;
        LeafNode previous = null;
        int max_keys = degree - 1;
        int min_keys = max_keys % 2 == 0 ? max_keys / 2 : 1 + max_keys / 2;

        @Override
        void delete(Integer key) throws Exception {
            // delete for LeafNode will call delete for InnerNode
            int ind = this.keys.indexOf((Integer) key);
            this.keys.remove(ind);
            this.list_data.remove(ind);

            if (this.isUnderflow() && !this.equals(firstLeafNode)) {
                /*
                 * --------------------------------------------------------- leave L is
                 * underflow: fix the size of L with transfer or merge
                 * ---------------------------------------------------------
                 */
                // to be siblings they must have the same parent
                LeafNode left_sibling = null;
                LeafNode right_sibling = null;

                if (this.previous != null) {
                    left_sibling = this.parent.equals(this.previous.parent) ? this.previous : null;
                }
                if (this.next != null) {
                    right_sibling = this.parent.equals(this.next.parent) ? this.next : null;
                }

                // check if previous sibling can give keys
                if ((left_sibling != null) && (left_sibling.keys.size() > left_sibling.min_keys)) {
                    // transfer last key and ptr from leftSibling(L) into L
                    this.keys.add(0, left_sibling.keys.get(left_sibling.keys.size() - 1));
                    this.list_data.add(0, left_sibling.list_data.get(left_sibling.list_data.size() - 1));
                    left_sibling.keys.remove(left_sibling.keys.size() - 1);
                    left_sibling.list_data.remove(left_sibling.list_data.size() - 1);
                    // update the key in innernode
                    this.parent.keys.set(this.parent.children.indexOf(this) - 1, this.keys.get(0));
                }
                // check if next sibling can give keys
                else if ((right_sibling != null) && (right_sibling.keys.size() > right_sibling.min_keys)) {
                    // transfer first key and ptr from rightSibling(L) into L
                    this.keys.add(right_sibling.keys.get(0));
                    this.list_data.add(right_sibling.list_data.get(0));
                    right_sibling.keys.remove(0);
                    right_sibling.list_data.remove(0);
                    // update the key in innernode
                    this.parent.keys.set(this.parent.children.indexOf(this), right_sibling.keys.get(0));

                }
                // check if next doesnt exist so we can merge with previous
                else if (left_sibling != null) {
                    // merge leftSibling(L) + L + L's last pointer into leftSibling(L);
                    for (int i = 0; i < this.keys.size(); i++) {
                        left_sibling.addData(this.keys.get(i), this.list_data.get(i));
                    }
                    // update next and previous
                    if (this.next != null) {
                        left_sibling.next = this.next;
                        this.next.previous = left_sibling;
                    }
                    // delete key and right subtree in parent node
                    Integer parentKey_ToDelete = this.parent.keys.get(this.parent.children.indexOf(this) - 1);
                    this.parent.delete(parentKey_ToDelete);
                    // delete L add return something to notice the garbage collector #TODO
                }
                // so next exists and we can merge with it
                else {
                    // no dead code
                    // Merge L + rightSibling(L) + last pointer into L;
                    for (int i = 0; i < right_sibling.keys.size(); i++) {
                        this.addData(right_sibling.keys.get(i), right_sibling.list_data.get(i));
                    }
                    // update next and previous
                    if (right_sibling.next != null) {
                        this.next = right_sibling.next;
                        right_sibling.next.previous = this;
                    }
                    // delete key and right subtree in parent node
                    Integer parentKey_ToDelete = this.parent.keys.get(this.parent.children.indexOf(right_sibling) - 1);
                    this.parent.delete(parentKey_ToDelete);
                    // delete L add return something to notice the garbage collector #TODO
                }

            }
            // else we are done
        }

        void addData(Integer key, Data data) {
            // search if key is in list keys, first yes
            // and
            // return index where it is, second no and
            // return index where we can insert
            // this methods automatically sort the list in order
            int binarySearch = Collections.binarySearch(keys, key);
            int idx = binarySearch >= 0 ? binarySearch : -binarySearch - 1;
            if (binarySearch >= 0) {
                // we can add more data to existing key
            } else {
                keys.add(idx, key);
                list_data.add(idx, data);
            }
        }

        @Override
        Node split() {
            // middle right element to promote technic

            // create brother node and add all right part of list
            LeafNode broNode = new LeafNode();
            int start = (keys.size() / 2);
            for (int i = start; i < keys.size(); i++) {
                broNode.addData(keys.get(i), list_data.get(i));
            }

            // clear right part of list
            keys.subList(start, keys.size()).clear();
            list_data.subList(start, keys.size()).clear();

            // family link
            if (this.next != null) {
                broNode.next = this.next;
                this.next.previous = broNode;
            }
            broNode.previous = this;
            this.next = broNode;
            return broNode;
        }

        @Override
        boolean isOverflow() {

            if (keys.size() > this.max_keys) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        boolean isUnderflow() {
            if (keys.size() < this.min_keys) {
                return true;
            } else {
                return false;
            }
        }
    }

    class InnerNode extends Node {

        ArrayList<Node> children = new ArrayList<Node>();
        int max_children = degree;
        int min_children = max_children % 2 == 0 ? max_children / 2 : 1 + max_children / 2;
        InnerNode next = null;
        InnerNode previous = null;

        void addKey(Integer key, Node child) throws Exception {
            // binarySearch returns index where we can insert
            // this methods automatically sort the list in order
            int binarySearch = Collections.binarySearch(keys, key);
            int idx = binarySearch >= 0 ? binarySearch : -binarySearch - 1;

            if (binarySearch >= 0) {
                throw new Exception("Key " + key + " already present");
            } else {
                keys.add(idx, key);
                // pb here
                children.add(idx + 1, child);
                child.parent = this;
            }
        }

        @Override
        void delete(Integer key) throws Exception {
            /*
             * Delete key and right children link from the node in B+-tree
             */
            this.children.remove(this.keys.indexOf(key) + 1);
            this.keys.remove(key);

            // check if its root
            if (this.equals(root)) {
                // if root is empty we delete it
                if (this.keys.size() == 0) {

                    InnerNode newRoot = (this.children.get(0) instanceof InnerNode) ? (InnerNode) this.children.get(0)
                            : null;
                    root = newRoot;
                }
            }
            // Check for underflow condition
            else if (this.isUnderflow()) {
                /*
                 * 
                 * InnerNode N is underflow: fix the size of N with transfer or merge
                 * 
                 */
                // to be siblings they must have the same parent
                InnerNode left_sibling = null;
                InnerNode right_sibling = null;

                if (this.previous != null) {
                    left_sibling = this.parent.equals(this.previous.parent) ? this.previous : null;
                }
                if (this.next != null) {
                    right_sibling = this.parent.equals(this.next.parent) ? this.next : null;
                }
                // check if previous sibling can give keys
                if ((left_sibling != null) && (left_sibling.children.size() > left_sibling.min_children)) {
                    // transfer last key from leftSibling(N) through parent into N as the first key;
                    this.keys.add(0, this.parent.keys.get(this.parent.children.indexOf(this) - 1));
                    this.parent.keys.set(this.parent.children.indexOf(this) - 1,
                            left_sibling.keys.get(left_sibling.keys.size() - 1));
                    // transfer right subtree link into N as the first link
                    this.children.add(0, left_sibling.children.get(left_sibling.children.size() - 1));
                    left_sibling.children.get(left_sibling.children.size() - 1).parent = this;
                    // deletes
                    left_sibling.keys.remove(left_sibling.keys.size() - 1);
                    left_sibling.children.remove(left_sibling.children.size() - 1);
                }
                // check if next sibling can give keys
                else if ((right_sibling != null) && (right_sibling.children.size() > right_sibling.min_children)) {
                    // transfer first key from rightSibling(N) through parent into N as the last
                    // key;
                    this.keys.add(this.parent.keys.get(this.parent.children.indexOf(this)));
                    this.parent.keys.set(this.parent.children.indexOf(this), right_sibling.keys.get(0));
                    // transfer left subtree link into N as the last link
                    this.children.add(right_sibling.children.get(0));
                    right_sibling.children.get(0).parent = this;
                    // deletes
                    right_sibling.keys.remove(0);
                    right_sibling.children.remove(0);

                }
                // check if next doesnt exist so we can merge with previous
                else if (left_sibling != null) {
                    // merge N with left sibling node
                    // Merge (1) leftSibling(N) + (2) key in parent node + (3) N
                    // into the leftSibling(N) node
                    Integer transferedKey = this.parent.keys.get((this.parent.children.indexOf(this) - 1));
                    left_sibling.keys.add(transferedKey);
                    left_sibling.children.add(this.children.get(0));
                    this.children.get(0).parent = left_sibling;
                    for (int i = 0; i < this.keys.size(); i++) {
                        left_sibling.addKey(this.keys.get(i), this.children.get(i + 1));
                    }
                    // Delete ( transfered key, right subtree ptr, parent(N) ) #recursive
                    this.parent.delete(transferedKey);
                }
                // so next exists and we can merge with it
                else {
                    // merge N with right sibling node
                    // Merge (1) N + (2) key in parent node + (3) rightSibling(N)
                    // into the node N;
                    Integer transferedKey = this.parent.keys.get((this.parent.children.indexOf(this)));
                    this.keys.add(transferedKey);
                    this.children.add(right_sibling.children.get(0));
                    right_sibling.children.get(0).parent = this;
                    for (int i = 0; i < right_sibling.keys.size(); i++) {
                        this.addKey(right_sibling.keys.get(i), right_sibling.children.get(i + 1));
                    }
                    // Delete ( transfered key, right subtree ptr, parent(N) ) #recursive
                    this.parent.delete(transferedKey);
                }

            }

        }

        @Override
        boolean isOverflow() {

            if (children.size() > this.max_children) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        Node split() throws Exception {
            // Inner node split
            // middle right element to promote technic

            // create brother node and add all right part of list and make them siblings
            InnerNode broNode = new InnerNode();

            if (this.next != null) {
                broNode.next = this.next;
                this.next.previous = broNode;
            }
            broNode.previous = this;
            this.next = broNode;

            int start = (keys.size() / 2);
            int key_to_promote = keys.get(start);
            for (int i = start; i < keys.size(); i++) {
                if (i != start) {
                    broNode.keys.add(keys.get(i));
                }
                broNode.children.add(children.get(i + 1));
                children.get(i + 1).parent = broNode;
            }

            // clear right part of list
            keys.subList(start, keys.size()).clear();
            children.subList(start + 1, children.size()).clear();

            if (parent == null) {

                // Create new root node
                InnerNode newRoot = new InnerNode();
                newRoot.children.add(this);
                newRoot.children.add(broNode);

                newRoot.keys.add(key_to_promote);
                this.parent = newRoot;
                broNode.parent = newRoot;

                root = newRoot;

            } else {

                this.parent.addKey(key_to_promote, broNode);

            }
            return this.parent;
        }

        @Override
        boolean isUnderflow() {
            if (children.size() < this.min_children) {
                return true;
            } else {
                return false;
            }
        }

    }

}
