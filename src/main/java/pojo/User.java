package pojo;

/**
 * @Description User <br>
 * @Author SpiderMao <br>
 * @Version 1.0 <br>
 * @CreateDate 2019/11/25 16:09 <br>
 * @See pojo <br>
 */
public class User {
    private int id;

    private String name;

    public User(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}
