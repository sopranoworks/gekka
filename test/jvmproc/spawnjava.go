/*
 * spawnjava.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// SpawnJava launches a fat JAR under jvmproc supervision. It exists so
// integration tests can replace `sbt runMain <Main>` with `java -cp <jar>
// <Main>`, eliminating sbt's bg-jobs/sbt_<hash> directory leak that
// accumulates whenever jvmproc SIGKILLs the sbt process group before
// sbt's shutdown hook runs.

package jvmproc

import (
	"context"
	"testing"
)

// buildJavaArgs lays out the argv for `java <jvmFlags...> -cp <jar>
// <mainClass> <mainArgs...>`. Pure function; covered by table tests.
func buildJavaArgs(jar, mainClass string, mainArgs, jvmFlags []string) []string {
	args := make([]string, 0, len(jvmFlags)+3+len(mainArgs))
	args = append(args, jvmFlags...)
	args = append(args, "-cp", jar, mainClass)
	args = append(args, mainArgs...)
	return args
}

// SpawnJava launches `java <opts.JVMFlags...> -cp <jar> <mainClass>
// <mainArgs...>` under the same supervision as Spawn (process group,
// SIGKILL on Stop, PortToRelease polling, atexit registry,
// OrchestratorTokenEnv injection).
//
// JVMFlags are placed on argv (plain `java` does not honor JAVA_OPTS the
// way sbt's launcher does); the underlying Spawn call therefore receives
// JVMFlags=nil so its env-var folding logic does nothing.
func SpawnJava(t testing.TB, ctx context.Context, jar, mainClass string,
	mainArgs []string, opts Options) (*Process, error) {
	t.Helper()
	args := buildJavaArgs(jar, mainClass, mainArgs, opts.JVMFlags)
	forwarded := opts
	forwarded.JVMFlags = nil
	return Spawn(t, ctx, "java", args, forwarded)
}
