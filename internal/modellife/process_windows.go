//go:build windows

package modellife

import (
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

type managedProcess struct {
	job windows.Handle
}

func prepareManagedCommand(_ *exec.Cmd) {}

func attachManagedProcess(cmd *exec.Cmd) (managedProcess, error) {
	if cmd == nil || cmd.Process == nil {
		return managedProcess{}, fmt.Errorf("missing process")
	}
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return managedProcess{}, fmt.Errorf("create job object: %w", err)
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	ret, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	)
	if err != nil || ret == 0 {
		_ = windows.CloseHandle(job)
		return managedProcess{}, fmt.Errorf("set job kill-on-close: %w", err)
	}
	process, err := windows.OpenProcess(
		windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE,
		false,
		uint32(cmd.Process.Pid),
	)
	if err != nil {
		_ = windows.CloseHandle(job)
		return managedProcess{}, fmt.Errorf("open child process: %w", err)
	}
	defer windows.CloseHandle(process)
	if err := windows.AssignProcessToJobObject(job, process); err != nil {
		_ = windows.CloseHandle(job)
		return managedProcess{}, fmt.Errorf("assign child to kill-on-close job: %w", err)
	}
	return managedProcess{job: job}, nil
}

func releaseManagedProcess(managed *managedProcess) error {
	if managed == nil || managed.job == 0 {
		return nil
	}
	err := windows.CloseHandle(managed.job)
	managed.job = 0
	return err
}

func terminateManagedProcess(cmd *exec.Cmd, grace time.Duration) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if grace <= 0 {
		grace = 8 * time.Second
	}
	pid := strconv.Itoa(cmd.Process.Pid)
	kill := exec.Command("taskkill.exe", "/PID", pid, "/T", "/F")
	treeErr := kill.Run()
	done := make(chan error, 1)
	go func() {
		_, err := cmd.Process.Wait()
		done <- err
	}()
	select {
	case <-done:
		// An exit status is expected after forced termination. The important
		// invariant is that Wait observed the managed process exit.
		return nil
	case <-time.After(grace):
		parentErr := cmd.Process.Kill()
		select {
		case <-done:
			if treeErr != nil {
				return fmt.Errorf("modellife: taskkill process tree: %w", errors.Join(treeErr, parentErr))
			}
			return parentErr
		case <-time.After(2 * time.Second):
			return fmt.Errorf("modellife: process tree pid %s did not exit: %w", pid, errors.Join(treeErr, parentErr))
		}
	}
}
