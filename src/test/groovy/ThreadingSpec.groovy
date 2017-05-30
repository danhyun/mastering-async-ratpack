import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class ThreadingSpec extends Specification {
  def "add numbers in parallel"() {
    given:
    def range = 1..100
    def sum = 0
    CountDownLatch latch = new CountDownLatch(range.size())

    when:
    range.collect { i ->
      return {
        println "Updating sum: was $sum is now $sum + $i"
        sum += i
        latch.countDown()
      }
    }.forEach(Thread.&start)

    and:
    latch.await()

    then:
    println "Parallel sum is $sum"
    sum != range.sum()
  }

  def "add numbers in parallel using atomicint"() {
    given:
    def range = 1..100
    def sum = new AtomicInteger(0)
    CountDownLatch latch = new CountDownLatch(range.size())

    when:
    range.collect { i ->
      return {
        println "Updating sum: was ${sum.get()} is now ${sum.addAndGet(i)}"
        latch.countDown()
      }
    }.forEach(Thread.&start)

    and:
    latch.await()

    then:
    sum.toBigInteger() == range.sum()
  }

  def "add numbers in parallel using object lock"() {
    given:
    def range = 1..100
    BigInteger sum = BigInteger.ZERO
    def e = Executors.newFixedThreadPool(50)

    when:
    range.forEach { i ->
      e.execute {
        synchronized (sum) {
          println "Updating sum: was $sum is now ${sum + i}"
          sum += i
        }
      }

    }

    and:
    e.shutdown()
    e.awaitTermination(1, TimeUnit.MINUTES)

    then:
    sum.toBigInteger() == range.sum()
  }
}
