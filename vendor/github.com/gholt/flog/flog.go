// Package flog provides a logging facility with the usual syslog-style levels
// of Critical, Error, Warning, Info, and Debug. It just uses os.Stderr and
// os.Stdout by default, and it's mostly here to provide a quick, easy, yet
// replaceable way to do logging.
package flog

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/gholt/brimio"
)

// Default is an instance of Flog for ease of use. Though I'd recommend writing
// your code to accept a Flog or Logger so others can alter it later; this
// Default can be changed to indirectly have a similar effect.
var Default = New(nil)

// CriticalPrintf is a convenience function equivalent to
// Default.CriticalPrintf.
func CriticalPrintf(format string, args ...interface{}) {
	Default.CriticalPrintf(format, args...)
}

// CriticalPrintln is a convenience function equivalent to
// Default.CriticalPrintln.
func CriticalPrintln(args ...interface{}) {
	Default.CriticalPrintln(args...)
}

// ErrorPrintf is a convenience function equivalent to Default.ErrorPrintf.
func ErrorPrintf(format string, args ...interface{}) {
	Default.ErrorPrintf(format, args...)
}

// ErrorPrintln is a convenience function equivalent to Default.ErrorPrintln.
func ErrorPrintln(args ...interface{}) {
	Default.ErrorPrintln(args...)
}

// WarningPrintf is a convenience function equivalent to Default.WarningPrintf.
func WarningPrintf(format string, args ...interface{}) {
	Default.WarningPrintf(format, args...)
}

// WarningPrintln is a convenience function equivalent to
// Default.WarningPrintln.
func WarningPrintln(args ...interface{}) {
	Default.WarningPrintln(args...)
}

// InfoPrintf is a convenience function equivalent to Default.InfoPrintf.
func InfoPrintf(format string, args ...interface{}) {
	Default.InfoPrintf(format, args...)
}

// InfoPrintln is a convenience function equivalent to Default.InfoPrintln.
func InfoPrintln(args ...interface{}) {
	Default.InfoPrintln(args...)
}

// DebugPrintf is a convenience function equivalent to Default.DebugPrintf.
func DebugPrintf(format string, args ...interface{}) {
	Default.DebugPrintf(format, args...)
}

// DebugPrintln is a convenience function equivalent to Default.DebugPrintln.
func DebugPrintln(args ...interface{}) {
	Default.DebugPrintln(args...)
}

// Sub is a convenience function equivalent to Default.Sub.
func Sub(c *Config) Flog {
	return Default.Sub(c)
}

// Flog provides a logging facility with the usual syslog-style levels of
// Critical, Error, Warning, Info, and Debug.
type Flog interface {
	// CriticalPrintf logs the result of fmt.Sprintf(format, args).
	CriticalPrintf(format string, args ...interface{})
	// CriticalPrintln logs the result of fmt.Sprintln(args).
	CriticalPrintln(args ...interface{})
	// ErrorPrintf logs the result of fmt.Sprintf(format, args).
	ErrorPrintf(format string, args ...interface{})
	// ErrorPrintln logs the result of fmt.Sprintln(args).
	ErrorPrintln(args ...interface{})
	// WarningPrintf logs the result of fmt.Sprintf(format, args).
	WarningPrintf(format string, args ...interface{})
	// WarningPrintln logs the result of fmt.Sprintln(args).
	WarningPrintln(args ...interface{})
	// InfoPrintf logs the result of fmt.Sprintf(format, args).
	InfoPrintf(format string, args ...interface{})
	// InfoPrintln logs the result of fmt.Sprintln(args).
	InfoPrintln(args ...interface{})
	// DebugPrintf logs the result of fmt.Sprintf(format, args).
	DebugPrintf(format string, args ...interface{})
	// DebugPrintln logs the result of fmt.Sprintln(args).
	DebugPrintln(args ...interface{})
	// Sub creates a new Flog instance based on this one, possibly with
	// overrides from the given Config. If Config.Name is set, it will be
	// appended to the original Flog instance's name.
	Sub(c *Config) Flog
}

// Config is used when creating a new Flog instance.
type Config struct {
	// Name will be used in the prefix of each log line.
	Name string
	// CriticalWriter will accept Critical level log output. If set to nil, the
	// default os.Stderr will be used. To discard all Critical level output,
	// set to an instance of brimio.NullIO.
	CriticalWriter io.Writer
	// ErrorWriter will accept Error level log output. If set to nil, the
	// default os.Stderr will be used. To discard all Error level output, set
	// to an instance of brimio.NullIO.
	ErrorWriter io.Writer
	// WarningWriter will accept Warning level log output. If set to nil, the
	// default os.Stderr will be used. To discard all Warning level output, set
	// to an instance of brimio.NullIO.
	WarningWriter io.Writer
	// InfoWriter will accept Info level log output. If set to nil, the default
	// os.Stdout will be used. To discard all Info level output, set to
	// an instance of brimio.NullIO.
	InfoWriter io.Writer
	// DebugWriter will accept Debug level log output. If set to nil, the
	// default is to discard all Debug level output.
	DebugWriter io.Writer
}

func resolveConfig(c *Config, f *flog) *Config {
	cfg := &Config{}
	if c != nil {
		*cfg = *c
	}
	if f != nil && f.name != "" {
		if cfg.Name != "" {
			cfg.Name = f.name + " " + cfg.Name
		} else {
			cfg.Name = f.name
		}
	}
	if cfg.CriticalWriter == nil {
		if f != nil {
			cfg.CriticalWriter = f.criticalWriter
		} else {
			cfg.CriticalWriter = os.Stderr
		}
	} else if _, ok := cfg.CriticalWriter.(*brimio.NullIO); ok {
		cfg.CriticalWriter = nil
	}
	if cfg.ErrorWriter == nil {
		if f != nil {
			cfg.ErrorWriter = f.errorWriter
		} else {
			cfg.ErrorWriter = os.Stderr
		}
	} else if _, ok := cfg.ErrorWriter.(*brimio.NullIO); ok {
		cfg.ErrorWriter = nil
	}
	if cfg.WarningWriter == nil {
		if f != nil {
			cfg.WarningWriter = f.warningWriter
		} else {
			cfg.WarningWriter = os.Stderr
		}
	} else if _, ok := cfg.WarningWriter.(*brimio.NullIO); ok {
		cfg.WarningWriter = nil
	}
	if cfg.InfoWriter == nil {
		if f != nil {
			cfg.InfoWriter = f.infoWriter
		} else {
			cfg.InfoWriter = os.Stdout
		}
	} else if _, ok := cfg.InfoWriter.(*brimio.NullIO); ok {
		cfg.InfoWriter = nil
	}
	if cfg.DebugWriter == nil {
		if f != nil {
			cfg.DebugWriter = f.debugWriter
		} else {
			cfg.DebugWriter = nil
		}
	} else if _, ok := cfg.DebugWriter.(*brimio.NullIO); ok {
		cfg.DebugWriter = nil
	}
	return cfg
}

type flog struct {
	lock           sync.Mutex
	criticalWriter io.Writer
	errorWriter    io.Writer
	warningWriter  io.Writer
	infoWriter     io.Writer
	debugWriter    io.Writer
	buf            []byte

	name           string
	criticalFormat string
	errorFormat    string
	warningFormat  string
	infoFormat     string
	debugFormat    string
}

// New returns a new Flog instance based on the Config given.
func New(c *Config) Flog {
	cfg := resolveConfig(c, nil)
	f := &flog{
		name:           cfg.Name,
		criticalWriter: cfg.CriticalWriter,
		errorWriter:    cfg.ErrorWriter,
		warningWriter:  cfg.WarningWriter,
		infoWriter:     cfg.InfoWriter,
		debugWriter:    cfg.DebugWriter,
	}
	if f.name == "" {
		f.criticalFormat = fmt.Sprintf("2006-01-02 15:05:05 CRITICAL ")
		f.errorFormat = fmt.Sprintf("2006-01-02 15:05:05 ERROR ")
		f.warningFormat = fmt.Sprintf("2006-01-02 15:05:05 WARNING ")
		f.infoFormat = fmt.Sprintf("2006-01-02 15:05:05 INFO ")
		f.debugFormat = fmt.Sprintf("2006-01-02 15:05:05 DEBUG ")
	} else {
		f.criticalFormat = fmt.Sprintf("2006-01-02 15:05:05 CRITICAL %s ", f.name)
		f.errorFormat = fmt.Sprintf("2006-01-02 15:05:05 ERROR %s ", f.name)
		f.warningFormat = fmt.Sprintf("2006-01-02 15:05:05 WARNING %s ", f.name)
		f.infoFormat = fmt.Sprintf("2006-01-02 15:05:05 INFO %s ", f.name)
		f.debugFormat = fmt.Sprintf("2006-01-02 15:05:05 DEBUG %s ", f.name)
	}
	return f
}

func (f *flog) CriticalPrintf(format string, args ...interface{}) {
	f.lock.Lock()
	flogPrintf(f.buf, f.criticalWriter, f.criticalFormat, format, args...)
	f.lock.Unlock()
}

func (f *flog) ErrorPrintf(format string, args ...interface{}) {
	f.lock.Lock()
	flogPrintf(f.buf, f.errorWriter, f.errorFormat, format, args...)
	f.lock.Unlock()
}

func (f *flog) WarningPrintf(format string, args ...interface{}) {
	f.lock.Lock()
	flogPrintf(f.buf, f.warningWriter, f.warningFormat, format, args...)
	f.lock.Unlock()
}

func (f *flog) InfoPrintf(format string, args ...interface{}) {
	f.lock.Lock()
	flogPrintf(f.buf, f.infoWriter, f.infoFormat, format, args...)
	f.lock.Unlock()
}

func (f *flog) DebugPrintf(format string, args ...interface{}) {
	f.lock.Lock()
	flogPrintf(f.buf, f.debugWriter, f.debugFormat, format, args...)
	f.lock.Unlock()
}

func (f *flog) CriticalPrintln(args ...interface{}) {
	f.lock.Lock()
	flogPrintln(f.buf, f.criticalWriter, f.criticalFormat, args...)
	f.lock.Unlock()
}

func (f *flog) ErrorPrintln(args ...interface{}) {
	f.lock.Lock()
	flogPrintln(f.buf, f.errorWriter, f.errorFormat, args...)
	f.lock.Unlock()
}

func (f *flog) WarningPrintln(args ...interface{}) {
	f.lock.Lock()
	flogPrintln(f.buf, f.warningWriter, f.warningFormat, args...)
	f.lock.Unlock()
}

func (f *flog) InfoPrintln(args ...interface{}) {
	f.lock.Lock()
	flogPrintln(f.buf, f.infoWriter, f.infoFormat, args...)
	f.lock.Unlock()
}

func (f *flog) DebugPrintln(args ...interface{}) {
	f.lock.Lock()
	flogPrintln(f.buf, f.debugWriter, f.debugFormat, args...)
	f.lock.Unlock()
}

func flogPrintf(buf []byte, out io.Writer, prefixFormat string, format string, args ...interface{}) {
	if out == nil {
		return
	}
	buf = time.Now().AppendFormat(buf[:0], prefixFormat)
	bufr := bytes.NewBuffer(buf)
	fmt.Fprintf(bufr, format, args...)
	buf = bufr.Bytes()
	if len(buf) == 0 || buf[len(buf)-1] != '\n' {
		buf = append(buf, '\n')
	}
	out.Write(buf)
}

func flogPrintln(buf []byte, out io.Writer, prefixFormat string, args ...interface{}) {
	if out == nil {
		return
	}
	buf = time.Now().AppendFormat(buf[:0], prefixFormat)
	bufr := bytes.NewBuffer(buf)
	fmt.Fprintln(bufr, args...)
	buf = bufr.Bytes()
	if len(buf) == 0 || buf[len(buf)-1] != '\n' {
		buf = append(buf, '\n')
	}
	out.Write(buf)
}

func (f *flog) Sub(c *Config) Flog {
	cfg := resolveConfig(c, f)
	return New(cfg)
}

// Logger is an interface to match the most common calls to the standard
// library's log.Logger.
type Logger interface {
	// Fatal is equivalent to Print() followed by a call to os.Exit(1).
	Fatal(args ...interface{})
	// Fatalf is equivalent to Printf() followed by a call to os.Exit(1).
	Fatalf(format string, args ...interface{})
	// Fatalln is equivalent to Println() followed by a call to os.Exit(1).
	Fatalln(args ...interface{})
	// Print calls Output to print to the standard logger. Arguments are
	// handled in the manner of fmt.Print.
	Print(args ...interface{})
	// Printf calls Output to print to the standard logger. Arguments are
	// handled in the manner of fmt.Printf.
	Printf(format string, args ...interface{})
	// Println calls Output to print to the standard logger. Arguments are
	// handled in the manner of fmt.Println.
	Println(args ...interface{})
}

type wrapper struct {
	printf  func(format string, args ...interface{})
	println func(args ...interface{})
}

func (w *wrapper) Fatal(args ...interface{}) {
	w.println(args)
	os.Exit(1)
}

func (w *wrapper) Fatalf(format string, args ...interface{}) {
	w.printf(format, args)
	os.Exit(1)
}

func (w *wrapper) Fatalln(args ...interface{}) {
	w.println(args)
	os.Exit(1)
}

func (w *wrapper) Print(args ...interface{}) {
	w.println(args)
}

func (w *wrapper) Printf(format string, args ...interface{}) {
	w.printf(format, args)
}

func (w *wrapper) Println(args ...interface{}) {
	w.println(args)
}

// CriticalLogger returns a Logger instance that will send its output to the
// Critical level of the Flog given.
func CriticalLogger(f Flog) Logger {
	return &wrapper{
		printf:  f.CriticalPrintf,
		println: f.CriticalPrintln,
	}
}

// ErrorLogger returns a Logger instance that will send its output to the Error
// level of the Flog given.
func ErrorLogger(f Flog) Logger {
	return &wrapper{
		printf:  f.ErrorPrintf,
		println: f.ErrorPrintln,
	}
}

// WarningLogger returns a Logger instance that will send its output to the
// Warning level of the Flog given.
func WarningLogger(f Flog) Logger {
	return &wrapper{
		printf:  f.WarningPrintf,
		println: f.WarningPrintln,
	}
}

// InfoLogger returns a Logger instance that will send its output to the Info
// level of the Flog given.
func InfoLogger(f Flog) Logger {
	return &wrapper{
		printf:  f.InfoPrintf,
		println: f.InfoPrintln,
	}
}

// DebugLogger returns a Logger instance that will send its output to the Debug
// level of the Flog given.
func DebugLogger(f Flog) Logger {
	return &wrapper{
		printf:  f.DebugPrintf,
		println: f.DebugPrintln,
	}
}
