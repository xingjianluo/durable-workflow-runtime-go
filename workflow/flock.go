//go:build !windows

package workflow

import (
	"os"
	"syscall"
)

// lockDir 对打开的目录 fd 加排他 advisory 锁（flock LOCK_EX），
// 串行化同一台机器上多个进程（多个 Engine/Store）对存储目录的写操作。
// 进程退出（含崩溃）后内核自动释放锁。
func lockDir(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
}

// unlockDir 释放目录锁。
func unlockDir(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
