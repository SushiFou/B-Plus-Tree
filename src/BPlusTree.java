import java.util.ArrayList;
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

    public int degree;
    public LinkedList<Integer> indexes = new LinkedList<Integer>();

    public BPlusTree(String file, int degree) throws IOException {
        
        this.degree = degree;
        this.indexes = this.read(file); 
    }

    public LinkedList<Integer> read(String file) throws IOException {

        LinkedList<Integer> to_return = new LinkedList<Integer>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            Boolean firstline = true;
            while ((line = br.readLine()) != null) {
                String[] value = line.split(";");
                if(firstline){
                    firstline = false;
                }
                else{
                    to_return.add(Integer.parseInt(value[0]));
                }
            }
        }
        return to_return;
    }
}
