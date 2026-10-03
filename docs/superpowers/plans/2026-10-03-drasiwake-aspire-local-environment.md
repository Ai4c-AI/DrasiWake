# Implementation Plan: DrasiWake Aspire Local Environment

**Goal:** Add a local-only .NET Aspire AppHost that starts DrasiWake Host alongside the existing Drasi Server and OpenClaw Gateway Compose projects, waits for both dependencies to become healthy, injects their discovered addresses and the Gateway token into Host, and safely stops only the Compose stacks it started.

**Architecture:** Keep both external `docker-compose.yml` files authoritative and unmodified. Add an Aspire AppHost and a testable local-environment orchestration layer. The orchestration layer validates configuration and Docker/Compose prerequisites, starts the two external Compose projects with `up --detach --wait`, discovers published ports with `docker compose port`, supplies Host configuration, and owns rollback and shutdown. The exact Aspire resource/lifecycle API must be established by the first implementation task and must satisfy the lifecycle/ownership contract in the approved design spec.

**Tech Stack:** .NET 10, .NET Aspire AppHost, Docker Compose CLI, existing xUnit v3 and Microsoft.Testing.Platform conventions, central package management.

**Global Constraints:**

- Aspire is development/test tooling only; production Host startup and deployment remain unchanged.
- Do not copy, generate, parse, or commit the Drasi repository's `.env`; do not print expanded Compose configuration.
- Do not modify either external Compose repository.
- Never pass secrets in process arguments, resource names, logs, or sanitized diagnostics. Pass `MODEL_PROVIDER_KEY` and `OPENCLAW_AUTH_TOKEN` only through the Docker CLI child-process environment; redact known values from captured output before logging.
- Fail on pre-existing Compose/container/port conflicts. Never adopt, stop, or clean up resources not started by this AppHost lifetime.
- Cleanup only the stacks started by this AppHost lifetime, in reverse start order; never use `down --volumes` or `down -v`.
- Do not enable the OpenClaw `with-tls` profile in the initial implementation.
- Keep Docker-facing logic behind an injectable command executor so unit tests need no Docker daemon or external repositories.

## Task 1: Establish Aspire Lifecycle and Compose Contracts

**Files:**

- `Directory.Packages.props`
- `src/DrasiWake.AppHost/DrasiWake.AppHost.csproj`
- `src/DrasiWake.AppHost/AppHost.cs`
- `src/DrasiWake.LocalEnvironment/DrasiWake.LocalEnvironment.csproj`
- `src/DrasiWake.LocalEnvironment/ComposeStackResource.cs`
- `src/DrasiWake.LocalEnvironment/ComposeStackResourceBuilderExtensions.cs`
- `tests/DrasiWake.LocalEnvironment.Tests/DrasiWake.LocalEnvironment.Tests.csproj`
- `tests/DrasiWake.LocalEnvironment.Tests/ComposeEnvironmentLifecycleTests.cs`
- `tests/DrasiWake.LocalEnvironment.Tests/AppHostResourceTests.cs`
- `DrasiWake.sln`

**Steps:**

1. Select an Aspire version compatible with the repository's .NET 10 SDK and pin all new package versions through `Directory.Packages.props`.
2. Create the AppHost, local-environment library, and focused test project; reference the Host project from the AppHost and the library from the test project, then register all projects in the solution. Keep the library as a regular project reference, not an Aspire service resource.
3. Build a minimal custom Compose-stack resource/lifecycle prototype using the selected Aspire APIs. Demonstrate dependency gating for a Host project and cleanup on both normal AppHost shutdown and canceled/failed startup. Do not yet invoke the real external Compose projects.
4. On the target Windows/Docker Desktop environment, record `docker compose version` and verify `up --wait --wait-timeout` support. Verify the shape of `docker compose port` output using a disposable health-checked fixture when Docker is available. Keep this check isolated from the Drasi/OpenClaw repositories and their data.
5. If the chosen Aspire API cannot reliably guarantee startup ordering, dependency readiness, and cleanup, adjust the custom resource/lifecycle design before implementing the coordinator; preserve all approved observable behavior.

**Verification:**

- `dotnet restore DrasiWake.sln` completes with the pinned Aspire packages.
- `dotnet build src/DrasiWake.AppHost/DrasiWake.AppHost.csproj -c Release` succeeds.
- `dotnet test tests/DrasiWake.LocalEnvironment.Tests/DrasiWake.LocalEnvironment.Tests.csproj -c Release` passes a prototype test demonstrating that the dependent Host cannot start before the synthetic stack reports ready and that cleanup runs on normal and canceled shutdown.
- Docker capability checks either pass or report an explicit local prerequisite; they must not touch external project state.

## Task 2: Add Configuration and Preflight Validation

**Files:**

- `src/DrasiWake.LocalEnvironment/ComposeEnvironmentOptions.cs`
- `src/DrasiWake.LocalEnvironment/ComposeEnvironmentValidator.cs`
- `tests/DrasiWake.LocalEnvironment.Tests/ComposeEnvironmentValidatorTests.cs`
- `Directory.Packages.props`
- `DrasiWake.sln`

**Steps:**

1. Extend the local-environment test project created in Task 1, following the repository's xUnit v3 and Microsoft.Testing.Platform conventions.
2. Define and bind the approved configuration keys for Drasi/OpenClaw repository paths, `ModelProviderKey`, and `AuthToken`.
3. Resolve relative repository paths against the DrasiWake repository root, not the process current directory; use sibling defaults `drasi-server` and `openclaw.net`.
4. Validate repository directories and `docker-compose.yml` files, required secret presence, Docker CLI/daemon availability, Compose `up --wait` capability, and known container/default-port conflicts before starting either stack. Obtain declared default port values from the Compose YAML only; never inspect the Drasi `.env`.
5. Return sanitized, actionable validation errors that name the missing setting or failed stage but never include secret values or full environment/configuration dumps.

**Tests:**

- Assert sibling defaults, absolute overrides, relative overrides against the repository root, and invalid/missing Compose paths.
- Assert each required secret is validated by name and its value is absent from diagnostics.
- Assert missing Docker/Compose capability or detected conflicts prevent any start/cleanup command.
- Assert a detected conflict on a Compose-declared default host port prevents startup without attempting to infer an override from `.env`.
- Assert validator logic never reads the Drasi `.env` file.

**Verification:** `dotnet test tests/DrasiWake.LocalEnvironment.Tests/DrasiWake.LocalEnvironment.Tests.csproj -c Release` passes without Docker or external repositories.

## Task 3: Implement the Safe Docker/Compose Command Boundary

**Files:**

- `src/DrasiWake.LocalEnvironment/IComposeCommandExecutor.cs`
- `src/DrasiWake.LocalEnvironment/ComposeCommandExecutor.cs`
- `src/DrasiWake.LocalEnvironment/ComposeCommand.cs`
- `tests/DrasiWake.LocalEnvironment.Tests/ComposeCommandExecutorTests.cs`

**Steps:**

1. Define a structured command request with executable, argument list, working directory, child-process environment, timeout/cancellation, and captured output. Never construct a shell command string.
2. Implement a production executor using `ProcessStartInfo.ArgumentList` and `Environment`; keep secrets out of arguments.
3. Redact configured secret values from stdout/stderr before returning or logging command results. Do not invoke commands that print expanded Compose configuration.
4. Return exit code and sanitized output to the coordinator; surface process launch, timeout, and cancellation failures without leaking raw environment values.
5. Implement a fake executor that records structured requests and returns deterministic outputs for unit tests.

**Tests:**

- Assert executable arguments contain no configured secret value while child environment contains the expected Compose variable mappings.
- Assert secrets echoed by fake stdout/stderr are redacted from all returned/logged text.
- Assert cancellation, timeout, nonzero exit, and process-start errors are represented without dumping environment variables.

**Verification:** Run the Task 2 focused test command; all command-boundary tests pass without launching Docker.

## Task 4: Implement Compose Preflight, Parallel Startup, and Rollback

**Files:**

- `src/DrasiWake.LocalEnvironment/ComposeStackDefinition.cs`
- `src/DrasiWake.LocalEnvironment/ComposeEnvironmentCoordinator.cs`
- `tests/DrasiWake.LocalEnvironment.Tests/ComposeEnvironmentCoordinatorTests.cs`

**Steps:**

1. Define the Drasi and OpenClaw stack metadata: repository root, compose file, explicit stable Compose project identity, expected service names, host/container ports, and the permitted secret environment for each stack.
2. Before `up`, verify the target Compose project has no existing containers, check known fixed service/container names, and probe the default host ports declared in the Compose YAML. Treat any pre-existing resource as a conflict; never run `down` against it. Compose startup remains authoritative for dynamic port overrides; do not read `.env`.
3. Start both stacks concurrently with `docker compose --project-directory <repo> -f <repo>\docker-compose.yml up --detach --wait --wait-timeout <seconds>`, passing secrets only in the relevant child environment.
4. Mark a stack as AppHost-owned only after its start attempt is attributable to this coordinator. If either stack fails, cancel/await the other start and clean up only stacks created by this attempt; do not start Host.
5. Keep diagnostics stage-specific and sanitized. Use the Compose health checks already defined in the external files; do not recreate health probes in the AppHost.

**Tests:**

- Assert both `up --wait` operations are launched concurrently and include the correct project directory/file and only the stack's allowed secret variables.
- Assert Host remains gated until both Compose commands report healthy completion.
- Assert failure/timeout in either stack rolls back only resources created during this attempt, waits for in-flight startup to settle, and never uses volume deletion flags.
- Assert a preflight conflict causes zero `up` and zero `down` calls, including when the other stack would otherwise start successfully.

**Verification:** `dotnet test tests/DrasiWake.LocalEnvironment.Tests/DrasiWake.LocalEnvironment.Tests.csproj -c Release` passes.

## Task 5: Discover Published Addresses and Wire the Host Resource

**Files:**

- `src/DrasiWake.LocalEnvironment/ComposePortParser.cs`
- `src/DrasiWake.LocalEnvironment/ComposeEnvironmentCoordinator.cs`
- `src/DrasiWake.AppHost/AppHost.cs`
- `src/DrasiWake.LocalEnvironment/ComposeStackResourceBuilderExtensions.cs`
- `tests/DrasiWake.LocalEnvironment.Tests/ComposePortParserTests.cs`
- `tests/DrasiWake.LocalEnvironment.Tests/ComposeEnvironmentCoordinatorTests.cs`

**Steps:**

1. After both stacks are healthy, query Drasi service `drasi-server` port `8080` and OpenClaw service `openclaw` port `18789` with `docker compose port` from their respective project directories.
2. Parse exactly one valid host/port mapping for each service. Normalize wildcard bind hosts (`0.0.0.0`, `::`, or equivalent) to a host-reachable loopback address. Reject missing, malformed, ambiguous, or invalid port results; do not guess defaults.
3. Construct the Drasi and OpenClaw base URIs from discovered values.
4. Inject `DrasiWake:Drasi:ServerUri`, `DrasiWake:OpenClaw:BaseAddress`, and `DrasiWake:OpenClaw:BearerToken` into the Host project resource using Aspire's supported configuration/environment API. Ensure the Host starts only after both Compose resources report ready and address discovery succeeds.
5. Preserve `DrasiWakeHostBuilder.CreateHost(IConfiguration)` and existing production configuration behavior; do not add Aspire dependencies to `DrasiWake.Host`.

**Tests:**

- Cover IPv4, IPv6, wildcard normalization, one-line and supported Windows output shapes, invalid port numbers, empty output, and ambiguous mappings.
- Assert discovered URIs and Bearer Token reach Host configuration exactly and Host is not launched on discovery failure.
- Assert startup failure after ports are discovered still rolls back only AppHost-owned Compose stacks.

**Verification:** `dotnet test tests/DrasiWake.LocalEnvironment.Tests/DrasiWake.LocalEnvironment.Tests.csproj -c Release` passes; `dotnet build src/DrasiWake.AppHost/DrasiWake.AppHost.csproj -c Release` succeeds.

## Task 6: Implement Ordered Shutdown and Ownership-Safe Cleanup

**Files:**

- `src/DrasiWake.LocalEnvironment/ComposeEnvironmentCoordinator.cs`
- `src/DrasiWake.LocalEnvironment/ComposeStackResourceBuilderExtensions.cs`
- `tests/DrasiWake.LocalEnvironment.Tests/ComposeEnvironmentCoordinatorTests.cs`

**Steps:**

1. Connect the selected Aspire shutdown/cancellation lifecycle to the coordinator, based on the verified Task 1 API.
2. Stop the Host project before stopping Compose dependencies; stop Compose stacks in reverse startup order.
3. Run `docker compose down` only for stacks marked as started by this AppHost lifetime. Never pass `--volumes`, `-v`, or broad Docker stop/remove commands.
4. Attempt cleanup of every owned stack even if an earlier cleanup fails; report sanitized per-stack cleanup outcomes in AppHost output.
5. On the next start after an interrupted process, treat surviving containers as pre-existing conflicts and fail safely rather than claiming ownership or cleaning them up.

**Tests:**

- Assert exact shutdown order: Host, then OpenClaw/Drasi in reverse startup order.
- Assert each owned stack receives one `down` without volume-removal flags and unowned stacks receive none.
- Assert cleanup continues after one stack's `down` failure and surfaces that failure without suppressing the other cleanup.
- Assert repeated stop/cancellation is idempotent and preserves ownership state.

**Verification:** Run the focused local-environment test project; all shutdown and startup-failure cleanup tests pass.

## Task 7: Add End-to-End AppHost and Host Regression Coverage

**Files:**

- `tests/DrasiWake.LocalEnvironment.Tests/AppHostResourceTests.cs`
- `tests/DrasiWake.IntegrationTests/HostStartupTests.cs`
- `DrasiWake.sln`

**Steps:**

1. Add an AppHost model test that verifies the Host project depends on both Compose stack resources and receives the three approved configuration keys without embedding secret values in resource metadata or arguments.
2. Add or extend Host startup tests to prove the existing settings continue to bind correctly from environment-style configuration keys.
3. Run the full solution test suite and Release build to verify production Host behavior is unchanged.
4. Keep tests deterministic and independent of Docker, external repositories, and real model-provider credentials.

**Verification:**

- `dotnet test DrasiWake.sln -c Release`
- `dotnet build DrasiWake.sln -c Release`
- Existing Bridge Core regression tests pass; no test requires a committed or printed secret.

## Task 8: Document Local Setup and Run the Real-Docker Smoke Test

**Files:**

- `README.md`
- `docs/development/aspire-local-environment.md`

**Steps:**

1. Document required sibling repository layout, path overrides, AppHost User Secrets/environment configuration for both keys, startup command, and expected dashboard resources.
2. Document Docker/Compose prerequisites, the no-takeover conflict behavior, cleanup semantics, preserved volumes, and the known host-network exposure risk from existing Compose port bindings.
3. Document that Aspire is not part of production deployment and that external Compose files remain the source of truth.
4. With Docker Desktop, both external repositories, and valid local secrets available, start the AppHost and verify Drasi/PostgreSQL and OpenClaw health precede Host startup; verify Host uses the discovered endpoints and configured Bearer Token.
5. Stop the AppHost and verify its containers stop while `drasi_postgres_data` and the OpenClaw memory volume remain. Verify a deliberately occupied port or pre-existing container yields a clear failure and is not stopped by AppHost.

**Verification:**

- `dotnet run --project src/DrasiWake.AppHost/DrasiWake.AppHost.csproj`
- Complete the four smoke checks in the approved design spec.
- `git diff --check` reports no whitespace errors.

**Environment-dependent gate:** The real-Docker smoke test requires Docker Desktop, the sibling Drasi Server and OpenClaw.NET repositories, and valid local `MODEL_PROVIDER_KEY` and `OPENCLAW_AUTH_TOKEN` values. If those prerequisites are absent, report the exact skipped check; do not claim the Compose lifecycle is end-to-end verified.

## Completion Criteria

- The Aspire AppHost starts the existing Drasi and OpenClaw Compose projects, waits for health, discovers published ports, and starts Host with the required addresses/token.
- Startup failures do not launch Host and only roll back stacks attributable to the current AppHost start attempt.
- Normal shutdown stops Host before Compose dependencies, preserves all named volumes, and attempts cleanup of both owned stacks.
- Unit tests prove preflight, path resolution, secret handling/redaction, readiness gating, port parsing, rollback, ownership, and shutdown semantics without Docker.
- Solution tests and Release build pass; the real-Docker smoke-test status is reported separately.
- No changes are made to external Drasi/OpenClaw repositories or production Host runtime dependencies.
