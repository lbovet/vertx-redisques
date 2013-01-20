package li.chee.vertx.redisques;

import org.junit.Test;
import org.vertx.java.framework.TestBase;

public class JavaRedisQuesTest extends TestBase {

    @Override
    protected void setUp() throws Exception {
      super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
      super.tearDown();
    }

    @Test
    public void testSimple() throws Exception {
      start(getMethodName());
    }

    @Test
    public void testMore() throws Exception {
      start(getMethodName());
    }    
    
    private void start(String methName) throws Exception {
      startApp(TestClient.class.getName());
      startApp(TestProcessor.class.getName());
      startTest(methName);
    }

}
