import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * @Description RuleMethodTest 配合@Rule <br>
 * @Author SpiderMao <br>
 * @Version 1.0 <br>
 * @CreateDate 2019/07/01 9:58 <br>
 * @See PACKAGE_NAME <br>
 */
public class RuleMethodTest implements TestRule {

    public Statement apply(Statement base, Description description) {
        return new StatementTest1(base);
    }

}
