
public class App {
    public static void main(String[] args) throws Exception {

        BPlusTree test = new BPlusTree("data.csv", 4);
        BPlusTree.LeafNode sibling = test.firstLeafNode;
        System.out.println("Root Keys : "+test.root.keys);
        // for (Node child : test.root.children) {
        //     System.out.println("Child Keys : "+child.keys);
        //     for (Node child2 : ((BPlusTree.InnerNode) child).children){
        //         System.out.println(child2.keys);
        //     }
        // }

        // test.print_Tree();
        
        while (sibling != null) {
            System.out.println(sibling.keys+" with parents : "+sibling.parent.keys);
            // System.out.println(sibling.parent.keys);
            sibling = sibling.next;
        }

    }
}
