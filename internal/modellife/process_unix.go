//go:build !windows

package modellife

import (
	"errors"
	"os/exec"
	"syscall"
	"time"
)

type managedProcess struct{}

func prepareManagedCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func attachManagedProcess(_ *exec.Cmd) (managedProcess, error) {
	return managedProcess{}, nil
}

func releaseManagedProcess(_ *managedProcess) error {
	return nil
}

func terminateManagedProcess(cmd *exec.Cmd, grace time.Duration) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	pid := cmd.Process.Pid
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	done := make(chan error, 1)
	go func() {
		_, err := cmd.Process.Wait()
		done <- err
	}()
	select {
	case err := <-done:
		if errors.Is(err, exec.ErrWaitDelay) {
			return nil
		}
		return err
	case <-time.After(grace):
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		return <-done
	}
}
