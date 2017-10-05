package com.wxmimperio.kafka;

import com.sun.deploy.util.StringUtils;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.Arrays;

/**
 * Unit test for simple App.
 */
public class AppTest
        extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(AppTest.class);
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp() {
        assertTrue(true);
    }


    @org.junit.Test
    public void test() {
        String[] str = "fasdfasd\tfasdfads\t46545646\tfads454df56ad4545\t1111111111".split("\\t", -1);

        System.out.println(str[str.length - 1]);

        String[] message = Arrays.copyOf(str, str.length - 1);

        System.out.println(Arrays.asList(message));

        System.out.println(StringUtils.join(Arrays.asList(message), "\t"));
    }
}
