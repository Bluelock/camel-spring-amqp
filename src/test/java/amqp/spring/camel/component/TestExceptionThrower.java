package amqp.spring.camel.component;

public class TestExceptionThrower {
    public void explode() {
        throw new RuntimeException();
    }
}
