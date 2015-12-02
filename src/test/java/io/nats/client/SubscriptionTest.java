package io.nats.client;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SubscriptionTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		fail("Not yet implemented");
	}
	
    UnitTestUtilities utils = new UnitTestUtilities();

//    [TestInitialize()]
//    public void Initialize()
//    {
//        utils.StartDefaultServer();
//    }
//
//    [TestCleanup()]
//    public void Cleanup()
//    {
//        utils.StopDefaultServer();
//    }
//
//    [TestMethod]
//    public void TestServerAutoUnsub()
//    {
//        using (IConnection c = new ConnectionFactory().CreateConnection())
//        {
//            long received = 0;
//            int max = 10;
//
//            using (IAsyncSubscription s = c.SubscribeAsync("foo"))
//            {
//                s.MessageHandler += (sender, arg) =>
//                {
//                    System.Console.WriteLine("Received msg.");
//                    received++;
//                };
//
//                s.AutoUnsubscribe(max);
//                s.Start();
//
//                for (int i = 0; i < (max * 2); i++)
//                {
//                    c.Publish("foo", Encoding.UTF8.GetBytes("hello"));
//                }
//                c.Flush();
//
//                Thread.Sleep(500);
//
//                if (received != max)
//                {
//                    Assert.Fail("Recieved ({0}) != max ({1})",
//                        received, max);
//                }
//                Assert.IsFalse(s.IsValid);
//            }
//        }
//    }
//
//    [TestMethod]
//    public void TestClientAutoUnsub()
//    {
//        using (IConnection c = new ConnectionFactory().CreateConnection())
//        {
//            long received = 0;
//            int max = 10;
//
//            using (ISyncSubscription s = c.SubscribeSync("foo"))
//            {
//                s.AutoUnsubscribe(max);
//
//                for (int i = 0; i < max * 2; i++)
//                {
//                    c.Publish("foo", null);
//                }
//                c.Flush();
//
//                Thread.Sleep(100);
//
//                try
//                {
//                    while (true)
//                    {
//                        s.NextMessage(0);
//                        received++;
//                    }
//                }
//                catch (NATSBadSubscriptionException) { /* ignore */ }
//
//                Assert.IsTrue(received == max);
//                Assert.IsFalse(s.IsValid);
//            }
//        }
//    }
//
//    [TestMethod]
//    public void TestCloseSubRelease()
//    {
//        using (IConnection c = new ConnectionFactory().CreateConnection())
//        {
//            using (ISyncSubscription s = c.SubscribeSync("foo"))
//            {
//                Stopwatch sw = new Stopwatch();
//                sw.Start();
//                try
//                {
//                    new Task(() => { Thread.Sleep(100); c.Close(); }).Start();
//                     s.NextMessage(10000);
//                }
//                catch (Exception) { /* ignore */ }
//
//                sw.Stop();
//
//                Assert.IsTrue(sw.ElapsedMilliseconds < 10000);
//            }
//        }
//    }
//
//    [TestMethod]
//    public void TestValidSubscriber()
//    {
//        using (IConnection c = new ConnectionFactory().CreateConnection())
//        {
//            using (ISyncSubscription s = c.SubscribeSync("foo"))
//            {
//                Assert.IsTrue(s.IsValid);
//
//                try { s.NextMessage(100); }
//                catch (NATSTimeoutException) { }
//
//                Assert.IsTrue(s.IsValid);
//
//                s.Unsubscribe();
//
//                Assert.IsFalse(s.IsValid);
//
//                try { s.NextMessage(100); }
//                catch (NATSBadSubscriptionException) { }
//            }
//        }
//    }
//
//    // TODO [TestMethod]
//    public void TestSlowSubscriber()
//    {
//        Options opts = ConnectionFactory.GetDefaultOptions();
//        opts.SubChannelLength = 10;
//
//        using (IConnection c = new ConnectionFactory().CreateConnection(opts))
//        {
//            using (ISyncSubscription s = c.SubscribeSync("foo"))
//            {
//                for (int i =0; i < (opts.SubChannelLength+100); i++)
//                {
//                    c.Publish("foo", null);
//                }
//
//                try
//                {
//                    c.Flush();
//                }
//                catch (Exception ex)
//                {
//                    System.Console.WriteLine(ex);
//                    if (ex.InnerException != null)
//                        System.Console.WriteLine(ex.InnerException);
//
//                    throw ex;
//                }
//
//                try 
//                {
//                    s.NextMessage();
//                }
//                catch (NATSSlowConsumerException)
//                {
//                    return;
//                }
//                Assert.Fail("Did not receive an exception.");
//            }
//        } 
//    }
//
//    [TestMethod]
//    public void TestSlowAsyncSubscriber()
//    {
//        Options opts = ConnectionFactory.GetDefaultOptions();
//        opts.SubChannelLength = 10;
//
//        using (IConnection c = new ConnectionFactory().CreateConnection(opts))
//        {
//            using (IAsyncSubscription s = c.SubscribeAsync("foo"))
//            {
//                Object mu = new Object();
//
//                s.MessageHandler += (sender, args) =>
//                {
//                    lock (mu)
//                    {
//                        Console.WriteLine("Subscriber Waiting....");
//                        Assert.IsTrue(Monitor.Wait(mu, 20000));
//                        Console.WriteLine("Subscriber done.");
//                    }
//                };
//
//                s.Start();
//
//                for (int i = 0; i < (opts.SubChannelLength + 100); i++)
//                {
//                    c.Publish("foo", null);
//                }
//
//                int flushTimeout = 1000;
//
//                Stopwatch sw = new Stopwatch();
//                sw.Start();
//
//                bool flushFailed = false;
//                try
//                {
//                    c.Flush(flushTimeout);
//                }
//                catch (Exception)
//                {
//                    flushFailed = true;
//                }
//
//                sw.Stop();
//
//                lock (mu)
//                {
//                    Monitor.Pulse(mu);
//                }
//
//                if (sw.ElapsedMilliseconds < flushTimeout)
//                {
//                    Assert.Fail("elapsed ({0}) < timeout ({1})",
//                        sw.ElapsedMilliseconds, flushTimeout);
//                }
//                
//                Assert.IsTrue(flushFailed);
//            }
//        }
//    }
//
//    [TestMethod]
//    public void TestAsyncErrHandler()
//    {
//        Object subLock = new Object();
//        object testLock = new Object();
//        IAsyncSubscription s;
//
//
//        Options opts = ConnectionFactory.GetDefaultOptions();
//        opts.SubChannelLength = 10;
//
//        bool handledError = false;
//
//        using (IConnection c = new ConnectionFactory().CreateConnection(opts))
//        {
//            using (s = c.SubscribeAsync("foo"))
//            {
//                opts.AsyncErrorEventHandler = (sender, args) =>
//                {
//                    lock (subLock)
//                    {
//                        if (handledError)
//                            return;
//
//                        handledError = true;
//
//                        Assert.IsTrue(args.Subscription == s);
//
//                        System.Console.WriteLine("Expected Error: " + args.Error);
//                        Assert.IsTrue(args.Error.Contains("Slow"));
//
//                        // release the subscriber
//                        Monitor.Pulse(subLock);
//                    }
//
//                    // release the test
//                    lock (testLock) { Monitor.Pulse(testLock); }
//                };
//
//                bool blockedOnSubscriber = false;
//                s.MessageHandler += (sender, args) =>
//                {
//                    lock (subLock)
//                    {
//                        if (blockedOnSubscriber)
//                            return;
//
//                        Console.WriteLine("Subscriber Waiting....");
//                        Assert.IsTrue(Monitor.Wait(subLock, 10000));
//                        Console.WriteLine("Subscriber done.");
//                        blockedOnSubscriber = true;
//                    }
//                };
//
//                s.Start();
//
//                lock(testLock)
//                {
//
//                    for (int i = 0; i < (opts.SubChannelLength + 100); i++)
//                    {
//                        c.Publish("foo", null);
//                    }
//                    c.Flush(1000);
//
//                    Assert.IsTrue(Monitor.Wait(testLock, 1000));
//                }
//            }
//        }
//    }
//
//    [TestMethod]
//    public void TestAsyncSubscriberStarvation()
//    {
//        Object waitCond = new Object();
//
//        using (IConnection c = new ConnectionFactory().CreateConnection())
//        {
//            using (IAsyncSubscription helper = c.SubscribeAsync("helper"),
//                                      start = c.SubscribeAsync("start"))
//            {
//                helper.MessageHandler += (sender, arg) =>
//                {
//                    System.Console.WriteLine("Helper");
//                    c.Publish(arg.Message.Reply,
//                        Encoding.UTF8.GetBytes("Hello"));
//                };
//                helper.Start();
//
//                start.MessageHandler += (sender, arg) =>
//                {
//                    System.Console.WriteLine("Responsder");
//	                string responseIB = c.NewInbox();
//                    IAsyncSubscription ia = c.SubscribeAsync(responseIB);
//
//                    ia.MessageHandler += (iSender, iArgs) =>
//                    {
//                        System.Console.WriteLine("Internal subscriber.");
//                        lock (waitCond) { Monitor.Pulse(waitCond); }
//                    };
//                    ia.Start();
//
//	                c.Publish("helper", responseIB,
//                        Encoding.UTF8.GetBytes("Help me!"));
//                };
//
//                start.Start();
//                 
//                c.Publish("start", Encoding.UTF8.GetBytes("Begin"));
//                c.Flush();
//
//                lock (waitCond) 
//                { 
//                    Assert.IsTrue(Monitor.Wait(waitCond, 2000));
//                }
//            }
//        }
//    }
//
//
//    [TestMethod]
//    public void TestAsyncSubscribersOnClose()
//    {
//        /// basically tests if the subscriber sub channel gets
//        /// cleared on a close.
//        Object waitCond = new Object();
//        int callbacks = 0;
//
//        using (IConnection c = new ConnectionFactory().CreateConnection())
//        {
//            using (IAsyncSubscription s = c.SubscribeAsync("foo"))
//            {
//                s.MessageHandler += (sender, args) =>
//                {
//                    callbacks++;
//                    lock (waitCond)
//                    {
//                        Monitor.Wait(waitCond);
//                    }
//                };
//
//                s.Start();
//
//                for (int i = 0; i < 10; i++)
//                {
//                    c.Publish("foo", null);
//                }
//                c.Flush();
//
//                Thread.Sleep(500);
//                c.Close();
//
//                lock (waitCond)
//                {
//                    Monitor.Pulse(waitCond);
//                }
//
//                Thread.Sleep(500);
//
//                Assert.IsTrue(callbacks == 1);
//            }
//        }
//    }

}
