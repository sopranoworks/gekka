package com.example

/**
 * OrchestratorGate is the Scala-side enforcement that scala-server mains
 * are only launched by the integration-test orchestrator (test/jvmproc),
 * never manually via `sbt run`.
 *
 * Every `main` / `extends App` body must call `OrchestratorGate.require()`
 * before doing anything else.  When the expected env var is missing or
 * has the wrong value, the server prints a "do not launch directly"
 * message and exits with code 78 (EX_CONFIG) so that a human bystander
 * sees a clear diagnostic instead of a hung process on port 2552.
 *
 * Pair file: `test/jvmproc/jvmproc.go` (Token must match TokenEnv value).
 *
 * Why a token rather than a presence check?  A presence check is too
 * easy to satisfy by mistake — anyone reading the error message could
 * `export GEKKA_ORCHESTRATOR_TOKEN=1` and bypass the gate.  Requiring a
 * specific value forces the operator to either:
 *   (a) use the test helper that already sets the right value, or
 *   (b) read the source and understand the contract before bypassing.
 */
object OrchestratorGate {
  // Must match jvmproc.OrchestratorTokenValue in test/jvmproc/jvmproc.go.
  // The literal value is intentionally non-obvious: anyone who sets it
  // manually has read the source and accepted the contract.
  private final val TokenEnv   = "GEKKA_ORCHESTRATOR_TOKEN"
  private final val TokenValue = "jvmproc-de0868c-2026"

  def require(): Unit = {
    val got = sys.env.getOrElse(TokenEnv, "")
    if (got != TokenValue) {
      Console.err.println(
        s"""[scala-server] DO NOT LAUNCH DIRECTLY.
           |
           |This Scala main is part of the integration-test fixture and is
           |spawned by the test orchestrator (test/jvmproc + startSbtServer).
           |Run the tests, not the server:
           |
           |  go test -tags integration ./...
           |  go test -tags integration -run TestIntegration_PekkoServer .
           |
           |Manual launches squat on port 2552 and hang the test goroutines.
           |
           |The required environment variable ($TokenEnv) is missing or has
           |the wrong value.  The orchestrator sets it; nobody else should.
           |""".stripMargin
      )
      sys.exit(78) // EX_CONFIG — configuration error, per sysexits.h.
    }
  }
}
