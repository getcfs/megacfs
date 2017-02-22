package dudu

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/gholt/brimtext"
)

var HELP_TEXT = errors.New(strings.TrimSpace(`
Estimate disk space usage with parallelism; tries to be GNU du compatible.

Currently supports the following options:

--apparent-size
-B --block-size=SIZE
-b --bytes
-h --human-readable
-k
-m
-s --summarize
--help

Also has the following options:

-v --verbose
--x-parallel-tasks=N
    Attempts will be made to concurrently examine files and directories up to
    the limit of N. Default for N is 100.
`))

func DUDU(args []string) error {
	cfg, items, err := parseArgs(args)
	if err != nil {
		return err
	}
	if cfg.verbosity > 1 {
		fmt.Printf("cfg is %#v\n", cfg)
		fmt.Printf("items are %#v\n", items)
	}

	fileCounts := make(map[string]int64)
	fileCountsLock := &sync.Mutex{}
	dirCounts := make(map[string]int64)
	dirCountsLock := &sync.Mutex{}

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

	statTasks := make(chan *statTask, cfg.parallelTasks)
	freeStatTasks := make(chan *statTask, cfg.parallelTasks)
	for i := 0; i < cfg.parallelTasks; i++ {
		freeStatTasks <- &statTask{}
		go statter(cfg, fileCounts, fileCountsLock, dirCounts, dirCountsLock, msgs, errs, wg, statTasks, freeStatTasks)
	}

	if len(items) == 0 {
		fi, err := os.Lstat(".")
		if err != nil {
			errs <- fmtErr(".", err)
		} else {
			ct := <-freeStatTasks
			ct.item = "."
			ct.fi = fi
			wg.Add(1)
			statTasks <- ct
		}
	} else {
		for _, item := range items {
			fi, err := os.Lstat(item)
			if err != nil {
				errs <- fmtErr(item, err)
				continue
			}
			ct := <-freeStatTasks
			ct.item = item
			ct.fi = fi
			wg.Add(1)
			statTasks <- ct
		}
	}

	wg.Wait()

	close(msgs)
	<-msgsDone
	close(errs)
	<-errsDone

	bsit := func(n int64) string {
		if cfg.humanReadable {
			return brimtext.HumanSize(n, "")
		}
		return fmt.Sprintf("%d", (n+cfg.blockSize-1)/cfg.blockSize)
	}

	if cfg.summarize && len(items) == 0 {
		items = []string{"."}
	}

	if len(items) == 0 {
		items := make([]string, len(dirCounts))
		i := 0
		for item, _ := range dirCounts {
			items[i] = item
			i++
		}
		sort.Sort(sort.Reverse(sort.StringSlice(items)))
		for _, item := range items {
			fmt.Printf("%s\t%s\n", bsit(dirCounts[item]), item)
		}
	} else {
		for _, item := range items {
			fi, err := os.Lstat(item)
			if err != nil {
				errs <- fmtErr(item, err)
				continue
			}
			if fi.IsDir() {
				if !cfg.summarize {
					prefix := item
					if item[len(item)-1] != '/' {
						prefix += "/"
					}
					subitems := make([]string, len(dirCounts))
					i := 0
					for subitem, _ := range dirCounts {
						if strings.HasPrefix(subitem, prefix) {
							subitems[i] = subitem
							i++
						}
					}
					subitems = subitems[:i]
					sort.Sort(sort.Reverse(sort.StringSlice(subitems)))
					for _, subitem := range subitems {
						fmt.Printf("%s\t%s\n", bsit(dirCounts[subitem]), subitem)
					}
				}
				subitem := item
				if item[len(item)-1] == '/' {
					subitem = item[:len(item)-1]
				}
				fmt.Printf("%s\t%s\n", bsit(dirCounts[subitem]), item)
			} else {
				fmt.Printf("%s\t%s\n", bsit(fileCounts[item]), item)
			}
		}
	}

	finalErrCount := atomic.LoadUint32(&errCount)
	if finalErrCount > 0 {
		return fmt.Errorf("there were %d errors", finalErrCount)
	}
	return nil
}

type config struct {
	verbosity     int
	blockSize     int64
	apparentSize  bool
	summarize     bool
	humanReadable bool
	parallelTasks int
	readdirBuffer int
	messageBuffer int
	errBuffer     int
}

func parseArgs(args []string) (*config, []string, error) {
	cfg := &config{
		verbosity:     0,
		blockSize:     1024,
		apparentSize:  false,
		summarize:     false,
		humanReadable: false,
		parallelTasks: 100,
		readdirBuffer: 1000,
		messageBuffer: 1000,
		errBuffer:     1000,
	}
	if os.Getenv("POSIXLY_CORRECT") != "" {
		cfg.blockSize = 512
	}
	if bs, err := strconv.Atoi(os.Getenv("BLOCKSIZE")); err == nil && bs > 0 {
		cfg.blockSize = int64(bs)
	}
	if bs, err := strconv.Atoi(os.Getenv("BLOCK_SIZE")); err == nil && bs > 0 {
		cfg.blockSize = int64(bs)
	}
	if bs, err := strconv.Atoi(os.Getenv("DU_BLOCK_SIZE")); err == nil && bs > 0 {
		cfg.blockSize = int64(bs)
	}
	var items []string
	mapitems := make(map[string]bool)
	for i := 0; i < len(args); i++ {
		if args[i] == "" {
			continue
		}
		if args[i][0] != '-' {
			if !mapitems[args[i]] {
				items = append(items, args[i])
				mapitems[args[i]] = true
			}
			continue
		}
		if args[i] == "-" {
			for _, item := range args[i+1:] {
				if !mapitems[item] {
					items = append(items, item)
					mapitems[item] = true
				}
			}
			continue
		}
		var opts []string
		if args[i][1] == '-' {
			opt := args[i][2:]
			if opt == "bytes" {
				opts = append(opts, "apparent-size", "block-size=1")
			} else {
				if !strings.Contains(opt, "=") {
					if opt == "B" || opt == "block-size" {
						i++
						if len(args) <= i {
							return nil, nil, fmt.Errorf("--block-size requires a parameter")
						}
						opt += "=" + args[i]
					}
				}
				opts = append(opts, opt)
			}
		} else {
			for _, s := range args[i][1:] {
				switch s {
				case 'b':
					opts = append(opts, "apparent-size", "block-size=1")
				case 'h':
					opts = append(opts, "human-readable")
				case 'k':
					opts = append(opts, "block-size=1024")
				case 'm':
					opts = append(opts, "block-size=1048576")
				case 's':
					opts = append(opts, "summarize")
				case 'v':
					opts = append(opts, "verbose")
				}
			}
		}
		for _, opt := range opts {
			var arg string
			s := strings.SplitN(opt, "=", 2)
			if len(s) > 1 {
				opt = s[0]
				arg = s[1]
			}
			switch opt {
			case "apparent-size":
				cfg.apparentSize = true
			case "block-size":
				if arg == "" {
					return nil, nil, fmt.Errorf("--block-size requires a parameter")
				}
				n, err := strconv.Atoi(arg)
				if err != nil {
					return nil, nil, fmt.Errorf("could not parse number %q for --block-size", arg)
				}
				if n < 1 {
					n = 1
				}
				cfg.blockSize = int64(n)
			case "help":
				return nil, nil, HELP_TEXT
			case "human-readable":
				cfg.humanReadable = true
			case "summarize":
				cfg.summarize = true
			case "verbose":
				cfg.verbosity++
			case "x-parallel-tasks":
				if arg == "" {
					return nil, nil, fmt.Errorf("--x-parallel-tasks requires a parameter")
				}
				n, err := strconv.Atoi(arg)
				if err != nil {
					return nil, nil, fmt.Errorf("could not parse number %q for --x-parallel-tasks", arg)
				}
				if n < 1 {
					n = 1
				}
				cfg.parallelTasks = n
			default:
				return nil, nil, fmt.Errorf("unknown option %q", opt)
			}
		}
	}
	return cfg, items, nil
}

type statTask struct {
	item string
	fi   os.FileInfo
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

func statter(cfg *config, fileCounts map[string]int64, fileCountsLock *sync.Mutex, dirCounts map[string]int64, dirCountsLock *sync.Mutex, msgs chan string, errs chan string, wg *sync.WaitGroup, statTasks chan *statTask, freeStatTasks chan *statTask) {
	var localTasks []*statTask
	for {
		var item string
		var fi os.FileInfo
		if i := len(localTasks); i > 0 {
			i--
			ct := localTasks[i]
			localTasks = localTasks[:i]
			select {
			case fct := <-freeStatTasks:
				fct.item = ct.item
				fct.fi = ct.fi
				statTasks <- fct
				continue
			default:
				item = ct.item
				fi = ct.fi
			}
		} else {
			ct := <-statTasks
			item = ct.item
			fi = ct.fi
			freeStatTasks <- ct
		}
		if cfg.verbosity > 0 {
			msgs <- fmt.Sprintf("Statting %s", item)
		}
		fiSize := fi.Size()
		if !cfg.apparentSize {
			if st, ok := fi.Sys().(*syscall.Stat_t); ok {
				fiSize = st.Blocks * 512
			}
		}
		if fi.IsDir() {
			dirCountsLock.Lock()
			dirCountItem := item
			for {
				dirCounts[dirCountItem] += fiSize
				if dirCountItem == "." || dirCountItem == "/" {
					break
				}
				dirCountItem = path.Dir(dirCountItem)
			}
			dirCountsLock.Unlock()
		} else {
			fileCountsLock.Lock()
			fileCounts[item] += fiSize
			fileCountsLock.Unlock()
			dirCountsLock.Lock()
			dirCountItem := path.Dir(item)
			for {
				dirCounts[dirCountItem] += fiSize
				if dirCountItem == "." || dirCountItem == "/" {
					break
				}
				dirCountItem = path.Dir(dirCountItem)
			}
			dirCountsLock.Unlock()
		}
		if fi.IsDir() {
			f, err := os.Open(item)
			if err != nil {
				errs <- fmtErr(item, err)
				wg.Done()
				continue
			}
			for {
				subfis, err := f.Readdir(cfg.readdirBuffer)
				for _, subfi := range subfis {
					subitem := path.Join(item, subfi.Name())
					wg.Add(1)
					select {
					case ct := <-freeStatTasks:
						ct.item = subitem
						ct.fi = subfi
						statTasks <- ct
					default:
						localTasks = append(localTasks, &statTask{
							item: subitem,
							fi:   subfi,
						})
					}
				}
				if err != nil {
					f.Close()
					if err != io.EOF {
						errs <- fmtErr(item, err)
					}
					break
				}
			}
		}
		wg.Done()
	}
}
