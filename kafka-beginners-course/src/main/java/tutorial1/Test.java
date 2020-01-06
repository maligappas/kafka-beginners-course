package tutorial1;

public class Test {
    public static void main(String[] args) {
        final String fName = "James";
        String lName = "Gosling";
        String name1 = fName + lName;
        String name2 = fName + "Gosling";
        String name3 = "James" + "Gosling";
        
        System.out.println(name1);
        System.out.println(name2);
        
        System.out.println(name2);
        System.out.println(name3);
        
        System.out.println(name1 == name2);
        System.out.println(name2 == name3);
    }
}