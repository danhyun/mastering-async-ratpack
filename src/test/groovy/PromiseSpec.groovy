import ratpack.exec.*
import ratpack.exec.util.ParallelBatch
import ratpack.exec.util.SerialBatch
import ratpack.func.Function
import ratpack.func.Pair
import ratpack.test.exec.ExecHarness
import spock.lang.AutoCleanup
import spock.lang.Specification
import spock.lang.Unroll

import java.time.DayOfWeek
import java.time.LocalDateTime
import java.time.format.TextStyle
import java.time.temporal.ChronoField

class PromiseSpec extends Specification {

  @AutoCleanup
  @Delegate
  ExecHarness execHarness = ExecHarness.harness()

  def "creating a promise"() {
    expect:
    run {
      // tag::create-sync[]
      boolean notExecuted = true
      Promise p = Promise.sync {
        notExecuted = false
      }

      assert notExecuted
      // end::create-sync[]
    }
  }

  def "creating and subscribing to a promise"() {
    // tag::unmanaged[]
    given:
    Promise<String> p = Promise.sync {
      println 'sync'
      'sync'
    }

    when:
    p.then { s ->
      assert s == 'sync'
    }

    then:
    thrown UnmanagedThreadException
    // end::unmanaged[]
  }

  def "creating and subscribing to a promise using ExecHarness"() {
    // tag::yieldSync[]
    given:
    Promise p = Promise.sync {
      println 'sync'
      'sync'
    }

    when:
    ExecResult result = yield { p }

    then:
    result.value == 'sync'
    // end::yieldSync[]

    and:
    run {
      // tag::runSync[]
      p.then { String s ->
        println 'then'
        assert s == 'sync'
      }
      // end::runSync[]
    }
  }

  def "creating a promise from a known calculated value"() {
    // tag::promise-value[]
    given:
    Promise p = Promise.value('value') // <1>

    when:
    ExecResult result = yield { p }

    then:
    result.value == 'value'
    // end::promise-value[]
  }

  // tag::third-party-async[]
  Thread externalThirdPartyAsyncLibraryWithCallback(Closure callback) {
    Thread.start {
      println 'Thread started'
      (1..5).each { i ->
        println(i)
        sleep(1000)
      }

      callback('async computation complete')
      println 'Thread finished'
    }
  }
  // end::third-party-async[]

  def "creating a blocking promise"() {
    // tag::blocking-get[]
    given:
    Promise p = Blocking.get {
      'from blocking'
    }

    when:
    ExecResult result = yield { p }

    then:
    result.value == 'from blocking'
    // end::blocking-get[]
  }

  def "blocking promises run on different threads"() {
    // tag::blocking-thread-name[]
    given:
    Closure getCurrentThreadName = {
      return Thread.currentThread().name.split('-')[1]
    }

    and:
    Promise p = Blocking.get {
      Thread.sleep(1000)
      getCurrentThreadName()
    } map { String nameFromBlocking ->
      String name = getCurrentThreadName()
      return [nameFromBlocking, name].join(' -> ')
    }

    when:
    ExecResult result = yield { p }

    then:
    result.value == 'blocking -> compute'
    // end::blocking-thread-name[]
  }

  def "adapting async libraries to promises"() {
    // tag::promise-async[]
    given:
    Promise p = Promise.async { Downstream downstream ->
      println 'async start'
      externalThirdPartyAsyncLibraryWithCallback(
        downstream.&success
      )
      println 'async end'
    }

    when:
    ExecResult result = yield { p }

    then:
    result.value == 'async computation complete'
    // end::promise-async[]
  }

  def "handling exceptions (stop processing)"() {
    // tag::exception[]
    given:
    Promise p = Promise.sync {
      throw new Exception("oh no")
    }

    when:
    yield { p }.valueOrThrow

    then:
    thrown Exception
    // end::exception[]

    // tag::promise-onerror[]
    when:
    p = Promise.sync {
      throw new Exception("oh no")
    }.onError { Exception e ->
      println e.message
    }.map {
      'map'
    }

    and:
    def value = yield { p }.valueOrThrow

    then:
    notThrown Exception
    value != 'map'
    value == null
    // end::promise-onerror[]
  }

  def "handling exceptions and continue processing"() {
    // tag::promise-maperror[]
    given:
    Promise p = Promise.sync {
      throw new Exception("oh no")
    } mapError { Throwable t ->
      'default value'
    } map({ String s -> s.toUpperCase()} as Function)

    when:
    String value = yield { p }.valueOrThrow

    then:
    notThrown Exception
    value == 'DEFAULT VALUE'
    // end::promise-maperror[]
  }

  def "promises are immutable"() {
    // tag::immutable[]
    given:
    boolean exception = false

    when:
    Promise p = Promise.sync { throw new Exception() }

    p.onError { // <1>
      exception = true
      println 'oops'
    }
    p.map { 'map' } // <2>

    and:
    yield { p }.valueOrThrow

    then:
    exception == false
    thrown Exception
    // end::immutable[]

    // tag::mapPromise[]
    when:
    p = Promise.sync { throw new Exception() }
      .onError { // <1>
      exception = true
      println 'oops'
    }
    .map { 'map' }

    and:
    yield { p }.valueOrThrow

    then:
    exception
    notThrown Exception
    // end::mapPromise[]
  }

  def "transforming promises"() {
    expect:
    run {
      // tag::promise-map[]
      Promise.value(3)
        .map { int i -> 'A' * i } // <1>
        .then { String s ->
          println 'from map'
          assert s == 'AAA'
        }
      // end::promise-map[]

      // tag::promise-flatmap[]
      Promise.value(3)
        .flatMap { int i -> // <1>
          Blocking.get { 'A' * i } // <2>
        }.then { String s -> // <3>
          println 'from flatMap blocking'
          assert s == 'AAA'
        }
      // end::promise-flatmap[]

      // tag::promise-blockingmap[]
      Promise.value(3)
        .blockingMap { int i -> // <1>
          'A' * i
        }.then { String s ->
          println 'from blockingMap'
          assert s == 'AAA'
        }
      // end::promise-blockingmap[]

      // tag::promise-flatmap-async[]
      Promise.value(3)
        .flatMap { int i ->
          Promise.async { Downstream d -> // <1>
            Thread.start {
              println 'starting thread'
              Thread.sleep(100)
              d.success('A' * i)
            }
          }
        }.then { String s ->
          println 'from flatMap async Thread'
          assert s == 'AAA'
        }
      // end::promise-flatmap-async[]
    }
  }

  def "promise composition using pairs"() {
    // tag::promise-composition-pair[]
    given:
    List<Map<String, Object>> users = [[name: 'danny', interests: ['dancing', 'cooking']]]
    Map<String, List<String>> interests = [dancing: ['fun'], cooking: ['a great catch'], kotlin: ['with it']]

    expect:
    run {
      Blocking.get {
        users.find { Map user -> user.name == 'danny' } // <1>
      }.right { Map user -> // <2>
        user.interests.collectMany { interest -> // <3>
          interests[interest]
        }
      }.map { Pair<Map, List<String>> pair -> // <4>
        "${pair.left.name} is ${pair.right.join(' and is ')}"
      }.then { String msg ->
        assert msg == 'danny is fun and is a great catch'
      }
    }
    // end::promise-composition-pair[]
  }

  @Unroll
  def "transform a list of promises into a promise of a list in #type"() {
    // tag::batch-basic[]
    given:
    Random random = new Random(1)
    Closure<Promise> getPrice = { int id -> // <1>
      Blocking.get {
       [id: id, price: random.nextInt(10)]
      }
    }

    expect:
    run {
      Promise.value(50)
        .map { int i -> (1..i) }
        .flatMap { ids ->
          List<Promise> promises = ids.collect { int id -> getPrice(id) } // <2>
          batch(promises).yield() // <3>
        }.map { List<Map<String, Object>> results -> // <4>
          results.price.sum()
        }.map { int price ->
          "Total is \$${price}"
        }.then { String msg ->
          assert msg == 'Total is $238'
        }
    }

    where:

    type        | batch // <5>
    'serial'    | SerialBatch.&of
    'parallel'  | ParallelBatch.&of
    // end::batch-basic[]
  }

  def "forking a new promise"() {
    // tag::fork[]
    given:
    List list = []
    Closure addToList = { // <1>
      println it
      list << it
      it
    }

    expect:
    run {
      Blocking.get {
        addToList('foo') // <2>
      }.right(
        Promise.async { Downstream d ->
          d.success(addToList('bar')) // <3>
        }.fork() // <4>
      ).map { Pair<String, String> pair ->
        pair.left + pair.right
      }.then { String msg ->
        assert list == ['bar', 'foo']  // <5>
        assert msg == 'foobar'
      }
    }
    // end::fork[]
  }

  def "conditional mapping"() {
    // tag::map-if[]
    given:
    Closure fizzbuzzer = { candidate -> // <1>
      Promise.sync {
          println "${LocalDateTime.now()} EXECUTING FIZZBUZZ for $candidate"
          candidate
        }
        .mapIf({i -> i instanceof Number && i % 3 == 0 && i % 5 == 0}, { 'fizzbuzz' }) // <2>
        .mapIf({i -> i instanceof Number && i % 3 == 0}, { 'fizz' }) // <2>
        .mapIf({i -> i instanceof Number && i % 5 == 0}, { 'buzz' }) // <2>
    }

    expect:
    run {
      Promise.value(15)
        .map { 1..it }
        .flatMap { range ->
          ParallelBatch.of(range.collect { fizzbuzzer(it) }).yield() // <3>
        }.then { list -> // <4>
          assert list == [
            1, 2, 'fizz', 4, 'buzz',
            'fizz', 7, 8, 'fizz', 'buzz',
            11, 'fizz', 13, 14, 'fizzbuzz'
          ]
        }
    }
    // end::map-if[]
  }

  def "routing conditionals"() {
    expect:
    run {
      // tag::route[]
      Promise.sync(LocalDateTime.&now)
        .route({ldt -> (ldt.get(ChronoField.MILLI_OF_SECOND) & 1) == 0}, { ldt -> // <1>
          println "TERMINATING: $ldt is even"
        }).map { ldt ->
          println "MAPPING LocalDateTime to String"
          "$ldt is odd"
        }.then { String msg ->
          println "SUCCESS: $msg"
        }
      // end::route[]
    }
  }

  def "throttling promise execution"() {
    // tag::throttle[]
    given:
    Throttle throttle = Throttle.ofSize(3) // <1>
    Closure fizzbuzzer = { candidate ->
      Blocking.get {
          Thread.sleep(2000)
          println "${LocalDateTime.now()} EXECUTING FIZZBUZZ for $candidate"
          candidate
        }
        .mapIf({i -> i instanceof Number && i % 3 == 0 && i % 5 == 0}, { 'fizzbuzz' })
        .mapIf({i -> i instanceof Number && i % 3 == 0}, { 'fizz' })
        .mapIf({i -> i instanceof Number && i % 5 == 0}, { 'buzz' })
        .throttled(throttle) // <2>
    }

    expect:
    run {
      Promise.value(15)
        .map { 1..it }
        .flatMap { range ->
          ParallelBatch.of(range.collect { fizzbuzzer(it) }).yield()
        }
        .then { list ->
          assert list == [
            1, 2, 'fizz', 4, 'buzz',
            'fizz', 7, 8, 'fizz', 'buzz',
            11, 'fizz', 13, 14, 'fizzbuzz'
          ]
        }
    }
    // end::throttle[]
  }

  def "spying on promises"() {
    expect:
    run {
      // tag::wiretap[]
      Promise.sync(LocalDateTime.&now)
        .map { ldt -> ldt.getDayOfWeek() }
        .wiretap { Result<DayOfWeek> r -> // <1>
          println "Found: ${r.value.getDisplayName(TextStyle.FULL_STANDALONE, Locale.KOREA)}" // <2>
        }.then { DayOfWeek dow -> // <3>
          println "Today is $dow"
        }
      // end::wiretap[]
    }
  }

  def "creating an operation"() {
    expect:
    run {
      // tag::operation[]
      Operation.of { // <1>
        println 'hello from operation'
      }.then { // <2>
        println 'nothing returned from operation'
      }
      // end::operation[]
    }
  }

  def "from operation to promise"() {
    expect:
    run {
      // tag::operation-to-promise[]
      Promise<Void> p = Operation.of {
        println 'hello from operation'
      }.promise() // <1>

      p.map { v ->
        assert v == null // <2>
        println "v is void $v"
        "promise"
      }.then { String msg -> // <3>
        println "We transformed from operation to $msg"
      }
      // end::operation-to-promise[]
    }
  }

  def "from promise to operation"() {
    expect:
    run {
      // tag::promise-to-operation[]
      Promise.value("foo")
        .operation { String msg -> // <1>
          println "found value $msg"
        }
        .then {
          println "no value emitted from operation"
          assert it == null // <2>
        }
      // end::promise-to-operation[]
    }
  }
}
