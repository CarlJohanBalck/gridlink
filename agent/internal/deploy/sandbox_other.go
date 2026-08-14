//go:build !darwin

package deploy

// sandboxCommand is a no-op off macOS: the native engine is macOS-only, so
// this exists only to keep the package building for the Linux/Pi agent.
func sandboxCommand(cmd []string) []string { return cmd }
