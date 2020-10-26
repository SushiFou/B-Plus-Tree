
public class App {
    public static void main(String[] args) throws Exception {

        BPlusTreeMulti test = new BPlusTreeMulti("data.csv", 4, true);

        test.print_Tree();

        Data result = test.search(122);
        System.out.println(result);

    }
}
