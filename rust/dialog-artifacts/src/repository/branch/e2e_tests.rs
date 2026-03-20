// TODO: These end-to-end tests require a remote emulator that has been removed
// during the refactoring from RemoteInvocation/Network to the site-based
// capability pattern. Re-enable once an S3 emulator or test site is available.
//
// The tests previously used:
// - `Route<RemoteAddress>` for in-memory remote emulation
// - `Environment<Volatile, Route<RemoteAddress>>` as the test environment
// - `RemoteInvocation` for dispatching remote operations
//
// In the new architecture, remote operations go through:
// - `capability.at(site).acquire(&env)` for authorization
// - `Provider<S3Invocation<Fx>>` for execution
//
// A test harness providing `Provider<Authorize<Fx, S3Access>>` and
// `Provider<S3Invocation<Fx>>` backed by in-memory storage is needed.
