


public interface Connection {
    /**
     * Send a message to the specified subject. The message body <strong>will not</strong>
     * be copied. The expected usage is something like:
     * <p><blockquote><pre>
     * nc = Nats.connect()
     * nc.publish("hello", "world".getBytes("UTF-8"))
     * </pre></blockquote></p>
     * @param  subject The subject to send the message to.
     * @param  body The message body.
     */
    public void publish(String subject, byte[] body);
}