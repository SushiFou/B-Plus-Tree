import java.text.DecimalFormat;
import java.text.NumberFormat;


public class App {
    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();
        BPlusTree test = new BPlusTree("generated_data.csv", 4);
        test.add_file("generated_data.csv");
        long endTime = System.currentTimeMillis();

        NumberFormat formatter = new DecimalFormat("#0.00000");
        System.out.println("Execution time is " + formatter.format((endTime - startTime) / 1000d) + " seconds");
        

        for(int i=1 ; i<500001;i++){
            test.delete(i);
        }
        test.print_Tree();

        // // test = new BPlusTree("generated_data_sorted.csv", 4);
        // // test.bulk_loading("generated_data_sorted.csv", 0.7f);


        // startTime = System.nanoTime();
        // Data result = test.search(500123);
        // endTime = System.nanoTime();
        // // System.out.println("Execution time is " + formatter.format((endTime - startTime) / 1000d) + " seconds");
        // System.out.println(endTime - startTime);
        
        // // System.out.println(result);

        

        // startTime = System.currentTimeMillis();
        // test = new BPlusTree("generated_data_sorted.csv", 100);
        // test.bulk_loading("generated_data_sorted.csv", 0.7f);
        // endTime = System.currentTimeMillis();

        
        // formatter = new DecimalFormat("#0.00000");
        // System.out.println("Execution time is " + formatter.format((endTime - startTime) / 1000d) + " seconds");
        // // test.print_Tree();


    }
}
