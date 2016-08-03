# Why Templating?

> Copyright See AUTHORS. All rights reserved.  

LocMap uses templated source files to generate specific *flavors* of itself,
ValueLocMap and GroupLocMap. So, why not just use `interface{}` for the
internal slices that hold entries? Well, the tradeoff in less complex and less
repeated code is less speed. The tradeoff is worth it if the translation from
`interface{}` to concrete type is rare enough, but LocMap is a library where
this translation is expected to occur quite often.

Some included [experimental benchmarks](experiment_test.go) tell the story:

```bash
$ go test -bench=. -benchtime=10s
PASS
BenchmarkInterfaceSliceAssignment-8         10000000000          1.91 ns/op
BenchmarkStructSliceAssignment-8            10000000000          1.50 ns/op
BenchmarkInterfaceSliceRetrieval-8          10000000000          1.36 ns/op
BenchmarkStructSliceRetrieval-8             10000000000          1.09 ns/op
BenchmarkInterfaceSliceRetrievalAndUsage-8  10000000000          2.18 ns/op
BenchmarkStructSliceRetrievalAndUsage-8     10000000000          1.88 ns/op
ok      github.com/gholt/locmap 101.458s
```

Obviously the operations being tested are quite fast, no matter the scheme
used; but we're expecting these operations to happen quite often, and causing
concurrency contention. Assignment and retrieval run about 20% faster and
retrieval+usage runs about 10% faster.

Granted, the rest of the program really determines whether the tradeoff is
worth it. The higher the number of assignments and retrievals per second and
the higher amount of concurrency weighs the tradeoff in the templated side's
favor. Another consideration is the amount of expected growth and shrinkage of
the LocMap, as splits and merges inside the LocMap involve many retrievals and
reassignments in bursts.

One other note is that creating a non-templatized version of the code would
require it be sprinkled with `if isGroup { do this } else { do that }` that
would actually execute at run time, but I'd expect that overhead would be
incredibly tiny.


# Why GoT?

When I realized I needed templated source code, I couldn't find anything I
really liked. I really wanted some officially recommended tool for the job, but
all I had was `go generate` and a mandate to run whatever.

In my search I found some tools that would fit my task, but seemed overly
complicated or had too specific a use case. Many assume I just wanted to change
the type being passed around, but I also wanted to have altered function
declarations and altered internal behavior between ValueLocMap and GroupLocMap.

So I made [GoT](https://github.com/gholt/got) which is pretty simple. 56 lines
in one source file that wraps the standard text/template generator, feeding it
whatever variables are specified on the command line.

I think the downsides are that the source files (I end them with `.got` by
convention) are a bit ugly with the templating directives sprinkled throughout
and that I can't use tools like gofmt on them.

But the upsides are that it's a really simple tool that shouldn't have any
weird side-effects and that it uses the standard Go templating library that
most Go coders should already know.


# Looking Back and Forward

I think using GoT worked out pretty well in practice. The source templatized
code isn't completely horrible to look at and neither is the generated code.
It's easy to wrap my head around, no ASTs being wandered through, no trying to
indirectly motivate the generator to do what I want, just plain text constructs
to reason about.

An ongoing annoyance, however, is that reported line numbers from panics refer
to the generated file, not the source templatized file. I don't know how many
times I've fixed a bug only to find out I need to migrate that fix back to the
template. I don't know of any elegant way around that, except maybe making my
editor yell at me if I try to modify a generated file (I embed `_GEN_`
somewhere in the file name for all of them).

Looking forward I hope that some official tool is created around this need. Or
maybe someone will recommend or I'll run across a better tool than GoT.

---

_If you have a comment on this page, pop a GitHub issue on this repository and
mention the file name._
