//go:build darwin

package deploy

import "os"

// enginePolicy is the sandbox-exec profile the Metal engine runs under. It
// replaces the container boundary that Docker provided on Linux: deny by
// default, then grant only what Metal inference actually needs.
//
// Verified during the engine spike: Metal runs at full speed under this policy
// (iokit-open is what makes GPU access possible; mach-lookup reaches the window
// server and Metal's helper services).
//
// The engine needs no outbound network at all — weights are downloaded by the
// AGENT, not the engine — so only local listening is granted. Both
// network-bind AND network-inbound are required: bind alone still fails
// listen() with "operation not permitted", which is how this profile was
// first found to be wrong.
const enginePolicy = `(version 1)
(deny default)
(allow process-exec)
(allow process-fork)
(allow sysctl-read)
(allow mach-lookup)
(allow iokit-open)
(allow file-read*)
(allow file-write* (subpath "/private/tmp") (subpath "/private/var/folders"))
(allow network-bind (local ip))
(allow network-inbound (local ip))
`

// sandboxCommand wraps an engine command in sandbox-exec. If the profile
// cannot be written the command is returned unwrapped: refusing to serve at
// all would be a worse failure than serving unsandboxed, and the agent logs
// loudly either way.
func sandboxCommand(cmd []string) []string {
	f, err := os.CreateTemp("", "gridlink-engine-*.sb")
	if err != nil {
		return cmd
	}
	defer f.Close()
	if _, err := f.WriteString(enginePolicy); err != nil {
		os.Remove(f.Name())
		return cmd
	}
	return append([]string{"/usr/bin/sandbox-exec", "-f", f.Name()}, cmd...)
}
