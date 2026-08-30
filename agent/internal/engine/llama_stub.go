//go:build !(darwin && arm64 && cgo)

package engine

// Supported reports whether this build can run models. False here: this is the
// portable stub for Linux, and for CGO_ENABLED=0 builds such as the Raspberry
// Pi cross-compile, which must stay pure Go.
func Supported() bool { return false }

// Load always fails on unsupported platforms. The agent declines deployments
// rather than pretending it can serve them.
func Load(Params) (Model, error) { return nil, ErrUnsupported }

// Silence is a no-op without an engine.
func Silence() {}

// GPUStats always fails here, leaving usable_vram_mb at 0 — which the proto
// defines as "refuse to place".
func GPUStats() (Stats, error) { return Stats{}, ErrUnsupported }
