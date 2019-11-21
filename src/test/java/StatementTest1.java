import org.junit.runners.model.Statement;

/**
 * @Description my statement 配合testRule <br>
 * statement是rule具体的执行方法 执行为链式顺序 ruleChain
 * @Author SpiderMao <br>
 * @Version 1.0 <br>
 * @CreateDate 2019/07/01 10:38 <br>
 * @See PACKAGE_NAME <br>
 */
public class StatementTest1 extends Statement {

    private final Statement base;

    public StatementTest1(Statement base) {
        this.base = base;
    }

    @Override
    public void evaluate() throws Throwable {
        System.out.println("before...sth..sth");
        try {
            base.evaluate();
        } finally {
            System.out.println("after...sth..sth");
        }
    }
}
