
import org.junit.Test;

import java.util.*;

/**
 * @Description TODO <br>
 * @Author SpiderMao <br>
 * @Version 1.0 <br>
 * @CreateDate 2019/06/29 11:10 <br>
 * @See PACKAGE_NAME <br>
 */
public class Test1 {

    @Test
    public void test() {
        String[] arr = new String[]{"1", "2", "3", "4", "5", "6"};
        List<String> list = Arrays.asList(arr);
        for (String str : list) {
            System.out.println(str);
        }
        System.out.println("################for 增强################");
        Iterator<String> itr = list.iterator();
        while (itr.hasNext()) {
            System.out.println(itr.next());
        }
    }

    @Test
    public void compareArrayListAndLinkedListSet() {
        int length = 4000000;
        List<String> arrayList = new ArrayList<String>(length);
        for (int i = 0; i < 100; i++) {
            arrayList.add("a");
        }
        LinkedList<String> linkedList = new LinkedList<String>(arrayList);
        // arrayList
        Long startTime1 = System.currentTimeMillis();
        for (int j = 0; j < length - arrayList.size(); j++) {
            arrayList.set(1, "b");
        }
        Long endTime1 = System.currentTimeMillis();
        System.out.println("ArrayList spend time:" + (endTime1 - startTime1));
        // linkedList
        Long startTime2 = System.currentTimeMillis();
        for (int j = 0; j < length - linkedList.size(); j++) {
            linkedList.set(1, "b");
        }
        Long endTime2 = System.currentTimeMillis();
        System.out.println("LinkedList spend time:" + (endTime2 - startTime2));
    }

    /***
     * @Description 高效率删除表元素
     * 首先arrayList删除元素效率低，不采用，采用linkedList，具体删除方法不同效率也不同
     * @Author spiderMao
     * @CreateDate 2019/6/29 15:20
     * @Param []
     * @Return void
     */
    @Test
    public void compareArrayListAndLinkedListDelete() {
        int length = 400000;
        List<Integer> arrayList = new ArrayList<Integer>(length);
        for (int i = 0; i < length; i++) {
            arrayList.add(i);
        }
        LinkedList<Integer> linkedList1 = new LinkedList<Integer>(arrayList);
        LinkedList<Integer> linkedList2 = new LinkedList<Integer>(arrayList);
        LinkedList<Integer> linkedList3 = new LinkedList<Integer>(arrayList);

        // 1
        Long startTime1 = System.currentTimeMillis();
        delete1(linkedList1);
        Long endTime1 = System.currentTimeMillis();
        System.out.println("delete1 spend time:" + (endTime1 - startTime1));
        // 2
        Long startTime2 = System.currentTimeMillis();
        delete1(linkedList2);
        Long endTime2 = System.currentTimeMillis();
        System.out.println("delete2 spend time:" + (endTime2 - startTime2));
        // 3
        Long startTime3 = System.currentTimeMillis();
        delete1(linkedList3);
        Long endTime3 = System.currentTimeMillis();
        System.out.println("delete3 spend time:" + (endTime3 - startTime3));

    }

    //使用list接口中定义的根据index位置删除指定元素
    public void delete1(List<Integer> lst) {
        int i = 0;
        while (i < lst.size()) {
            if (lst.get(i) % 2 == 0) {
                lst.remove(i);
            } else {
                i++;
            }
        }
    }

    //使用collection接口中定义的根据内容删除指定元素
    public void delete2(List<Integer> lst) {
        for (Integer val : lst) {
            if (val % 2 == 0) {
                lst.remove(val);
            }
        }
    }

    //使用iterator(迭代器)接口定义的remove方法
    public void delete3(List<Integer> lst) {
        Iterator<Integer> itr = lst.iterator();
        while (itr.hasNext()) {
            if (itr.next() % 2 == 0) {
                itr.remove();
            }
        }
    }

}
