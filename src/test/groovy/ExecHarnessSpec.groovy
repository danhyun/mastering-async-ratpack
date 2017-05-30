import spock.lang.Specification
import ratpack.test.exec.ExecHarness
import ratpack.exec.ExecResult
import ratpack.exec.Promise

class ExecHarnessSpec extends Specification {
  def 'can yield'() {
    // tag::execHarnessYield[]
    given:
    ExecHarness execHarness = ExecHarness.harness() // <1>

    when:
    ExecResult result = execHarness.yield { // <2>
      Promise.value('ratpack') // <3>
    }

    then:
    result.value == 'ratpack' // <4>

    cleanup:
    execHarness.close() // <5>
    // end::execHarnessYield[]
  }

  def 'can yield single'() {
    // tag::execHarnessYieldSingle[]
    when:
    ExecResult result = ExecHarness.yieldSingle { // <1>
      Promise.value('ratpack') // <2>
    }

    then:
    result.value == 'ratpack' // <3>
    // end::execHarnessYieldSingle[]
  }

  def 'can run'() {
    // tag::execHarnessRun[]
    given:
    ExecHarness execHarness = ExecHarness.harness() // <1>

    expect:
    execHarness.run { // <2>
      Promise.value('ratpack') // <3>
        .then { String value -> // <4>
          assert value == 'ratpack' // <5>
        }
    }

    cleanup:
    execHarness.close() // <6>
    // end::execHarnessRun[]
  }

  def 'can run single'() {
    // tag::execHarnessRunSingle[]
    expect:
    ExecHarness.runSingle { // <1>
      Promise.value('ratpack') // <2>
        .then { String value -> // <3>
          assert value == 'ratpack' // <4>
        }
    }
    // end::execHarnessRunSingle[]
  }
}
