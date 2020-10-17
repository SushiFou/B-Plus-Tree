import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Map;
import java.util.Set;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;

public class BPlusTree {

    // degree = Max branching factor (=max_children per node)
    // So Max keys = Max branching factor - 1

    int degree;
    InnerNode root = null;
    LeafNode firstLeafNode;

    BPlusTree(String file, int degree) throws NumberFormatException, Exception {

        this.degree = degree;
        this.firstLeafNode = new LeafNode();
        add_file(file);
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
        LeafNode freeLeaf = (this.root == null) ? this.firstLeafNode : findLeafNode(this.root, key);
        // add into
        freeLeaf.addData(key, data);
        // if the add generated an overflow
        if (freeLeaf.isOverflow()) {

            Node broNode = freeLeaf.split();

            // split for the first time
            if (freeLeaf.parent == null) {
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
                freeLeaf.parent.addKey(broNode.keys.get(0), broNode);

                if (freeLeaf.parent.isOverflow()) {
                    
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

    private LeafNode findLeafNode(InnerNode node, int key) {

        int i;
        // Find next node on path to appropriate leaf node
        for (i = 0; i < this.degree - 1; i++) {
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

    private class LeafNode extends Node {

        ArrayList<Data> list_data = new ArrayList<Data>();
        LeafNode next = null;
        LeafNode previous = null;
        int max_keys = degree - 1;
        InnerNode parent = null;

        @Override
        Data getData(Integer key) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        void deleteData(Integer key) {
            // TODO Auto-generated method stub

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
                System.out.println("we add : " + key);
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
            if(this.next!=null){
                broNode.next = this.next;
                this.next.previous = broNode;
            }
            broNode.previous = this;
            this.next = broNode;
            return broNode;
        }

        @Override
        boolean isOverflow() {

            if (keys.size() > max_keys) {
                return true;
            } else {
                return false;
            }
        }
    }

    private class InnerNode extends Node {

        ArrayList<Node> children = new ArrayList<Node>();
        int max_keys = degree - 1;
        int max_children = degree;
        InnerNode parent = null;

        void addKey(Integer key, Node child) throws Exception {
            // binarySearch returns index where we can insert
            // this methods automatically sort the list in order
            int binarySearch = Collections.binarySearch(keys, key);
            int idx = binarySearch >= 0 ? binarySearch : -binarySearch - 1;
            if (binarySearch >= 0) {
                throw new Exception("Key " + key + " already present");
            } else {
                System.out.println("we add : " + key);
                keys.add(idx, key);
                children.add(idx, child);
            }
        }

        @Override
        Data getData(Integer key) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        void deleteData(Integer key) {
            // TODO Auto-generated method stub

        }

        @Override
        boolean isOverflow() {

            if (children.size() > max_children) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        Node split() throws Exception {
            // Inner node split
            // middle right element to promote technic

            // create brother node and add all right part of list
            InnerNode broNode = new InnerNode();
            int start = (keys.size() / 2);
            for (int i = start; i < keys.size(); i++) {
                broNode.addKey(keys.get(i), children.get(i));
            }

            // clear right part of list
            keys.subList(start, keys.size()).clear();
            children.subList(start, keys.size()).clear();

            if (parent == null) {

                // Create new root node
                InnerNode newRoot = new InnerNode();
                newRoot.children.add(this);
                newRoot.children.add(broNode);
                
                newRoot.keys.add(broNode.keys.get(0));
                this.parent = newRoot;
                broNode.parent = newRoot;

                root = newRoot;
    
            } else {
                
                this.parent.addKey(broNode.keys.get(0), broNode);
                
            }
            
            return this.parent;
        }

    }

}
