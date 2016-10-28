package findfind

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

	"github.com/danwakefield/fnmatch"
)

var HELP_TEXT = errors.New(strings.TrimSpace(`
Searches for files in a directory hierarchy; tries to be GNU find compatible.

Currently supports the following options:

( )
-a -and
-help --help
-iname
-name
! -not
-o -or
-type
    Only 'd' and 'f' for now; and everything not a dir matches 'f', for now.
-print

Also has the following options:

-v --verbose
--x-parallel-tasks=N
    Attempts will be made to concurrently examine files and directories up to
    the limit of N. Default for N is 100.
`))

func FindFind(args []string) error {
	cfg, items, err := parseArgs(args)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		items = []string{"."}
	}
	if cfg.verbosity > 1 {
		fmt.Printf("cfg is %#v\n", cfg)
		fmt.Printf("items are %#v\n", items)
	}

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
		go statter(cfg, msgs, errs, wg, statTasks, freeStatTasks)
	}

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
	matcher       func(string, os.FileInfo) bool
	parallelTasks int
	readdirBuffer int
	messageBuffer int
	errBuffer     int
}

func parseArgs(args []string) (*config, []string, error) {
	cfg := &config{
		verbosity:     0,
		matcher:       func(string, os.FileInfo) bool { return true },
		parallelTasks: 100,
		readdirBuffer: 1000,
		messageBuffer: 1000,
		errBuffer:     1000,
	}
	var items []string
	mapitems := make(map[string]bool)
	for i := 0; i < len(args); i++ {
		if args[i] == "" {
			continue
		}
		switch args[i] {
		case "-h", "-help", "--help":
			return nil, nil, HELP_TEXT
		case "-v", "--verbose":
			cfg.verbosity++
			continue
		case "-vv":
			cfg.verbosity += 2
			continue
		case "--x-parallel-tasks":
			if i+1 < len(args) {
				return nil, nil, errors.New("--x-parallel-tasks requires a value")
			}
			i++
			var err error
			cfg.parallelTasks, err = strconv.Atoi(args[i])
			if err != nil {
				return nil, nil, fmt.Errorf("cannot parse value given to --x-parallel-tasks: %q", args[i])
			}
			continue
		}
		c := args[i][0]
		switch c {
		case '!', '-', '(', ')':
			var leftoverArgs []string
			var err error
			leftoverArgs, cfg.matcher, err = generateMatcher(args[i:], false)
			if err != nil {
				return cfg, items, err
			}
			if len(leftoverArgs) != 0 {
				err = fmt.Errorf("leftover args: %#v", leftoverArgs)
			}
			i = len(args)
		default:
			if !mapitems[args[i]] {
				items = append(items, args[i])
				mapitems[args[i]] = true
			}
		}
	}
	if cfg.verbosity > 0 {
		fmt.Println("+----------[ matcher ]----------")
		cfg.matcher("|", nil)
		fmt.Println("+-------------------------------")
	}
	return cfg, items, nil
}

func generateMatcher(args []string, justOne bool) ([]string, func(string, os.FileInfo) bool, error) {
	var fa func(string, os.FileInfo) bool
	var fb func(string, os.FileInfo) bool
	var err error
	for len(args) > 0 && (!justOne || fa == nil) {
		switch args[0] {
		case "(":
			args, fb, err = generateMatcher(args[1:], false)
			if err != nil {
				return args, fb, err
			}
			if len(args) == 0 || args[0] != ")" {
				return nil, nil, errors.New("unbalanced parenthesis")
			}
			args = args[1:]
			if fa == nil {
				fa = fb
			} else {
				fc := fa
				fd := fb
				fa = func(item string, fi os.FileInfo) bool {
					if fi == nil {
						fmt.Println(item, "&&")
						item += "  "
						fc(item, fi)
						return fd(item, fi)
					}
					return fc(item, fi) && fd(item, fi)
				}
			}
		case ")":
			return args, fa, nil
		case "!", "-not":
			args, fb, err = generateMatcher(args[1:], true)
			if err != nil {
				return args, fb, err
			}
			if fa == nil {
				fc := fb
				fa = func(item string, fi os.FileInfo) bool {
					if fi == nil {
						fmt.Println(item, "!")
						item += "  "
						return !fc(item, fi)
					}
					return !fc(item, fi)
				}
			} else {
				fc := fa
				fd := fb
				fa = func(item string, fi os.FileInfo) bool {
					if fi == nil {
						fmt.Println(item, "&&")
						item += "  "
						fc(item, fi)
						fmt.Println(item, "!")
						item += "  "
						return !fd(item, fi)
					}
					return fc(item, fi) && !fd(item, fi)
				}
			}
		case "-a", "-and":
			args = args[1:]
		case "-o", "-or":
			args, fb, err = generateMatcher(args[1:], false)
			if err != nil {
				return args, fb, err
			}
			if fa == nil {
				return nil, nil, errors.New("-or with nothing before it")
			} else {
				fc := fa
				fd := fb
				fa = func(item string, fi os.FileInfo) bool {
					if fi == nil {
						fmt.Println(item, "||")
						item += "  "
						fc(item, fi)
						return fd(item, fi)
					}
					return fc(item, fi) || fd(item, fi)
				}
			}
		case "-iname":
			if len(args) == 1 {
				return nil, nil, errors.New("-iname requires a value")
			}
			iname := args[1]
			args = args[2:]
			fb = func(item string, fi os.FileInfo) bool {
				if fi == nil {
					fmt.Println(item, "iname", iname)
					return fnmatch.Match(iname, path.Base(item), fnmatch.FNM_CASEFOLD)
				}
				return fnmatch.Match(iname, path.Base(item), fnmatch.FNM_CASEFOLD)
			}
			if fa == nil {
				fa = fb
			} else {
				fc := fa
				fd := fb
				fa = func(item string, fi os.FileInfo) bool {
					if fi == nil {
						fmt.Println(item, "&&")
						item += "  "
						fc(item, fi)
						return fd(item, fi)
					}
					return fc(item, fi) && fd(item, fi)
				}
			}
		case "-name":
			if len(args) == 1 {
				return nil, nil, errors.New("-name requires a value")
			}
			name := args[1]
			args = args[2:]
			fb = func(item string, fi os.FileInfo) bool {
				if fi == nil {
					fmt.Println(item, "name", name)
					return fnmatch.Match(name, path.Base(item), 0)
				}
				return fnmatch.Match(name, path.Base(item), 0)
			}
			if fa == nil {
				fa = fb
			} else {
				fc := fa
				fd := fb
				fa = func(item string, fi os.FileInfo) bool {
					if fi == nil {
						fmt.Println(item, "&&")
						item += "  "
						fc(item, fi)
						return fd(item, fi)
					}
					return fc(item, fi) && fd(item, fi)
				}
			}
		case "-type":
			if len(args) == 1 {
				return nil, nil, errors.New("-type requires a value")
			}
			typ := args[1]
			switch typ {
			case "d", "f":
			default:
				return nil, nil, fmt.Errorf("unknown -type %q", typ)
			}
			args = args[2:]
			fb = func(item string, fi os.FileInfo) bool {
				if fi == nil {
					fmt.Println(item, "type", typ)
					return true
				}
				switch typ {
				case "d":
					return fi.IsDir()
				case "f":
					return !fi.IsDir()
				}
				panic("this error path should have been handle by the outside switch typ")
			}
			if fa == nil {
				fa = fb
			} else {
				fc := fa
				fd := fb
				fa = func(item string, fi os.FileInfo) bool {
					if fi == nil {
						fmt.Println(item, "&&")
						item += "  "
						fc(item, fi)
						return fd(item, fi)
					}
					return fc(item, fi) && fd(item, fi)
				}
			}
		case "-print":
			// -print is always used for now
		default:
			return args, fa, fmt.Errorf("unsure how to continue: %s", args)
		}
	}
	return args, fa, nil
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

func statter(cfg *config, msgs chan string, errs chan string, wg *sync.WaitGroup, statTasks chan *statTask, freeStatTasks chan *statTask) {
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
		if cfg.matcher(item, fi) {
			msgs <- item
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
					if item == "." || strings.HasPrefix(item, "./") {
						subitem = "./" + subitem
					}
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
