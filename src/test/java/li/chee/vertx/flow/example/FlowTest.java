package li.chee.vertx.flow.example;

import org.junit.Test;
import org.vertx.java.framework.TestBase;

public class FlowTest extends TestBase {

    @Override
    protected void setUp() throws Exception {
      super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
      super.tearDown();
    }

    @Test
    public void testIt() throws Exception {
      start(getMethodName());
    }
    
    private void start(String methName) throws Exception {
      startApp(MainVerticle.class.getName());
      startTest(methName);
    }

}
