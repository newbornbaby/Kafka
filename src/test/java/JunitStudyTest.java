import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * @Description Junit annotations
 * @Author SpiderMao <br>
 * @Version 1.0 <br>
 * @CreateDate 2019/07/01 9:31 <br>
 * @See PACKAGE_NAME <br>
 */
@RunWith(Parameterized.class)
public class JunitStudyTest {

    private int param1;
    private int param2;
    private int sum;

    @Rule
    public RuleMethodTest ruleMethodTest = new RuleMethodTest();

//    @Rule
//    public TemporaryFolder folder = new TemporaryFolder();


    public JunitStudyTest(int param1, int param2, int sum) {
        this.param1 = param1;
        this.param2 = param2;
        this.sum = sum;
    }

    @Before
    public void beforeTest() {
        System.out.println("before test.");
    }

    @After
    public void afterTest() {
        System.out.println("after test.");
    }

    @Ignore
    public void notFinished() {
        System.out.println("Ignore function.");
    }

    @Parameterized.Parameters(name = "{index}: {0} + {1} = {2}")
    public static Collection<Object[]> initParams() {
        return Arrays.asList(new Object[][]{{0, 0, 0}});
    }

    @Test
    public void test() throws IOException {
        assertEquals(sum, param1 + param2);
        System.out.println("test function.");
    }
}
