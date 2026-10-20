//go:build !linux

package executor

import "os/exec"

// Verification registration rejects non-Linux execution. This keeps all
// existing SDK/server builds available on other platforms.
func configureVerificationProcess(cmd *exec.Cmd) {}
