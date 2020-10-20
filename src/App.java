
public class App {
    public static void main(String[] args) throws Exception {

        BPlusTree test = new BPlusTree("data.csv", 4);

        test.print_Tree();

        Data result = test.search(122);
        System.out.println(result);

    }
}
