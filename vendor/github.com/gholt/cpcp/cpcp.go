package cpcp

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

var HELP_TEXT = errors.New(strings.TrimSpace(`
Copies files and directories with parallelism; tries to be GNU cp compatible.

Currently supports the following options:

-a --archive
-d
--help
-L --dereference
-P --no-dereference
--preserve=ATTR_LIST
    Current support in ATTR_LIST for links,mode,all.
-R -r --recursive
-v --verbose

Also has the following options:

--x-parallel-tasks=N
    Attempts will be made to concurrently copy files and directories up to the
    limit of N. Default for N is 100.
`))

func CPCP(args []string) error {
	cfg, srcs, dst, err := parseArgs(args)
	if err != nil {
		return err
	}
	if cfg.verbosity > 1 {
		fmt.Printf("cfg is %#v\n", cfg)
		fmt.Printf("srcs are %#v\n", srcs)
		fmt.Printf("dst is %#v\n", dst)
	}

	u := syscall.Umask(0)
	syscall.Umask(u)
	cfg.umask = os.FileMode(u & 0x1ff)

	msgs := make(chan string, cfg.messageBuffer)
	msgsDone := make(chan struct{})
	go func() {
		for {
			msg := <-msgs
			if msg == "" {
				break
			}
			fmt.Println(msg)
		}
		close(msgsDone)
	}()

	var errCount uint32
	errs := make(chan string, cfg.errBuffer)
	errsDone := make(chan struct{})
	go func() {
		for {
			err := <-errs
			if err == "" {
				break
			}
			atomic.AddUint32(&errCount, 1)
			fmt.Println(err)
		}
		close(errsDone)
	}()

	wg := &sync.WaitGroup{}

	copyTasks := make(chan *copyTask, cfg.parallelTasks)
	freeCopyTasks := make(chan *copyTask, cfg.parallelTasks)
	for i := 0; i < cfg.parallelTasks; i++ {
		freeCopyTasks <- &copyTask{}
		go copier(cfg, msgs, errs, wg, copyTasks, freeCopyTasks)
	}

	if len(srcs) == 1 {
		src := srcs[0]
		dstfi, err := os.Stat(dst)
		if err == nil && dstfi.IsDir() {
			dst = path.Join(dst, path.Base(src))
		}
		var srcfi os.FileInfo
		if cfg.dereference {
			srcfi, err = os.Stat(src)
		} else {
			srcfi, err = os.Lstat(src)
		}
		if err != nil {
			errs <- fmtErr(src, err)
		} else if srcfi.IsDir() && !cfg.recursive {
			errs <- fmt.Sprintf("omitting directory %q", src)
		} else {
			ct := <-freeCopyTasks
			ct.src = src
			ct.dst = dst
			ct.srcfi = srcfi
			wg.Add(1)
			copyTasks <- ct
		}
	} else {
		dstfi, err := os.Stat(dst)
		if err != nil && !os.IsNotExist(err) {
			errs <- fmtErr(dst, err)
		} else if os.IsNotExist(err) || !dstfi.IsDir() {
			errs <- fmt.Sprintf("target %q is not a directory", dst)
		} else {
			for _, src := range srcs {
				var srcfi os.FileInfo
				if cfg.dereference {
					srcfi, err = os.Stat(src)
				} else {
					srcfi, err = os.Lstat(src)
				}
				if err != nil {
					errs <- fmtErr(src, err)
					continue
				}
				if srcfi.IsDir() && !cfg.recursive {
					errs <- fmt.Sprintf("omitting directory %q", src)
					continue
				}
				ct := <-freeCopyTasks
				ct.src = src
				ct.dst = path.Join(dst, path.Base(src))
				ct.srcfi = srcfi
				wg.Add(1)
				copyTasks <- ct
			}
		}
	}

	wg.Wait()

	close(msgs)
	<-msgsDone
	close(errs)
	<-errsDone
	finalErrCount := atomic.LoadUint32(&errCount)
	if finalErrCount > 0 {
		return fmt.Errorf("there were %d errors", finalErrCount)
	}
	return nil
}

type config struct {
	verbosity     int
	dereference   bool
	recursive     bool
	preserveLinks bool
	preserveMode  bool
	messageBuffer int
	errBuffer     int
	parallelTasks int
	readdirBuffer int
	copyBuffer    int

	umask os.FileMode
}

func parseArgs(args []string) (*config, []string, string, error) {
	cfg := &config{
		verbosity:     0,
		dereference:   true,
		recursive:     false,
		preserveLinks: false,
		preserveMode:  false,
		messageBuffer: 1000,
		errBuffer:     1000,
		parallelTasks: 100,
		readdirBuffer: 1000,
		copyBuffer:    65536,
	}
	var srcs []string
	for i := 0; i < len(args); i++ {
		if args[i] == "" || args[i][0] != '-' {
			srcs = append(srcs, args[i])
			continue
		}
		if args[i] == "-" {
			srcs = append(srcs, args[i+1:]...)
			continue
		}
		var opts []string
		if args[i][1] == '-' {
			opt := args[i][2:]
			if !strings.Contains(opt, "=") {
				if opt == "preserve" {
					i++
					if len(args) <= i {
						return nil, nil, "", fmt.Errorf("--preserve requires a parameter")
					}
					opt += "=" + args[i]
				}
			}
			opts = append(opts, opt)
		} else {
			for _, s := range args[i][1:] {
				switch s {
				case 'a':
					opts = append(opts, "archive")
				case 'd':
					opts = append(opts, "no-dereference", "preserve=links")
				case 'L':
					opts = append(opts, "dereference")
				case 'P':
					opts = append(opts, "no-dereference")
				case 'R', 'r':
					opts = append(opts, "recursive")
				case 'v':
					opts = append(opts, "verbose")
				}
			}
		}
		var nopts []string
		for _, opt := range opts {
			if opt == "archive" {
				nopts = append(nopts, "no-dereference", "recursive", "preserve=all")
			} else {
				nopts = append(nopts, opt)
			}
		}
		opts = nopts
		for _, opt := range opts {
			var arg string
			s := strings.SplitN(opt, "=", 2)
			if len(s) > 1 {
				opt = s[0]
				arg = s[1]
			}
			switch opt {
			case "dereference":
				cfg.dereference = true
			case "help":
				return nil, nil, "", HELP_TEXT
			case "no-dereference":
				cfg.dereference = false
			case "preserve":
				if arg == "" {
					return nil, nil, "", fmt.Errorf("--preserve requires a parameter")
				}
				preserves := strings.Split(arg, ",")
				for _, preserve := range preserves {
					switch preserve {
					case "":
					case "links":
						cfg.preserveLinks = true
					case "mode":
						cfg.preserveMode = true
					case "all":
						cfg.preserveLinks = true
						cfg.preserveMode = true
					default:
						return nil, nil, "", fmt.Errorf("unsupported preserve specification %q\n", preserve)
					}
				}
			case "recursive":
				cfg.recursive = true
			case "verbose":
				cfg.verbosity++
			case "x-parallel-tasks":
				if arg == "" {
					return nil, nil, "", fmt.Errorf("--x-parallel-tasks requires a parameter")
				}
				n, err := strconv.Atoi(arg)
				if err != nil {
					return nil, nil, "", fmt.Errorf("could not parse number %q for --x-parallel-tasks", arg)
				}
				if n < 1 {
					n = 1
				}
				cfg.parallelTasks = n
			default:
				return nil, nil, "", fmt.Errorf("unknown option %q", opt)
			}
		}
	}
	switch len(srcs) {
	case 0:
		return nil, nil, "", errors.New("nothing specified to copy")
	case 1:
		return nil, nil, "", fmt.Errorf("missing destination parameter after %q", srcs[0])
	}
	return cfg, srcs[:len(srcs)-1], srcs[len(srcs)-1], nil
}

type copyTask struct {
	src   string
	dst   string
	srcfi os.FileInfo
}

func fmtErr(pth string, err error) string {
	rv := err.Error()
	if rv == "" {
		rv = "unknown error"
	}
	if pth != "" {
		rv = pth + ": " + rv
	}
	_, filename, line, ok := runtime.Caller(1)
	if ok {
		rv = fmt.Sprintf("%s @%s:%d", rv, path.Base(filename), line)
	}
	return rv
}

func copier(cfg *config, msgs chan string, errs chan string, wg *sync.WaitGroup, copyTasks chan *copyTask, freeCopyTasks chan *copyTask) {
	var localTasks []*copyTask
	copyBuf := make([]byte, cfg.copyBuffer)
	for {
		var src string
		var dst string
		var srcfi os.FileInfo
		if i := len(localTasks); i > 0 {
			i--
			ct := localTasks[i]
			localTasks = localTasks[:i]
			select {
			case fct := <-freeCopyTasks:
				fct.src = ct.src
				fct.dst = ct.dst
				fct.srcfi = ct.srcfi
				copyTasks <- fct
				continue
			default:
				src = ct.src
				dst = ct.dst
				srcfi = ct.srcfi
			}
		} else {
			ct := <-copyTasks
			src = ct.src
			dst = ct.dst
			srcfi = ct.srcfi
			freeCopyTasks <- ct
		}
		if cfg.verbosity > 0 {
			msgs <- fmt.Sprintf("%s -> %s", src, dst)
		}
		if srcfi.IsDir() {
			if !cfg.recursive {
				errs <- fmt.Sprintf("omitting directory %q", src)
				continue
			}
			m := srcfi.Mode()
			if !cfg.preserveMode {
				m &= cfg.umask
			}
			if err := os.Mkdir(dst, m); err != nil {
				if !os.IsExist(err) {
					errs <- fmtErr(dst, err)
				}
			}
			// The above Mkdir doesn't always seem to apply the exact mode we
			// asked it to.
			if cfg.preserveMode {
				if err := os.Chmod(dst, m); err != nil {
					errs <- fmtErr(dst, err)
				}
			}
			f, err := os.Open(src)
			if err != nil {
				errs <- fmtErr(src, err)
				wg.Done()
				continue
			}
			for {
				fis, err := f.Readdir(cfg.readdirBuffer)
				for _, fi := range fis {
					subsrc := path.Join(src, fi.Name())
					subdst := path.Join(dst, fi.Name())
					wg.Add(1)
					select {
					case ct := <-freeCopyTasks:
						ct.src = subsrc
						ct.dst = subdst
						ct.srcfi = fi
						copyTasks <- ct
					default:
						localTasks = append(localTasks, &copyTask{
							src:   subsrc,
							dst:   subdst,
							srcfi: fi,
						})
					}
				}
				if err != nil {
					f.Close()
					if err != io.EOF {
						errs <- fmtErr(src, err)
					}
					break
				}
			}
		} else if srcfi.Mode().IsRegular() {
			srcf, err := os.Open(src)
			if err != nil {
				errs <- fmtErr(src, err)
			} else {
				dstf, err := os.Create(dst)
				if err != nil {
					errs <- fmtErr(dst, err)
				} else {
					_, err := io.CopyBuffer(dstf, srcf, copyBuf)
					if err != nil {
						errs <- fmtErr(dst, err)
					}
					srcf.Close()
					dstf.Close()
					m := srcfi.Mode()
					if !cfg.preserveMode {
						m &= cfg.umask
					}
					if err := os.Chmod(dst, m); err != nil {
						errs <- fmtErr(dst, err)
					}
				}
			}
		} else if srcfi.Mode()|os.ModeSymlink != 0 {
			target, err := os.Readlink(src)
			if err != nil {
				errs <- fmtErr(src, err)
			} else if err = os.Symlink(target, dst); err != nil {
				if os.IsExist(err) {
					if err = os.Remove(dst); err != nil {
						errs <- fmtErr(dst, fmt.Errorf("destination of symlink already exists and cannot be removed: %s", err))
					} else if err = os.Symlink(target, dst); err != nil {
						errs <- fmtErr(dst, err)
					}
				} else {
					errs <- fmtErr(dst, err)
				}
			}
		}
		wg.Done()
	}
}
