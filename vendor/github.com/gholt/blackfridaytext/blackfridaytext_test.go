// Copyright Gregory Holt. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package blackfridaytext

import "testing"

func TestBasic(t *testing.T) {
	in := "Basic Test"
	out := string(MarkdownToTextNoMetadata([]byte(in), nil))
	exp := "Basic Test\n"
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}
}

func TestMetadata(t *testing.T) {
	in := `One: Two  
Three: Four  
`
	out, pos := MarkdownMetadata([]byte(in))
	exp := [][]string{
		[]string{"One", "Two"},
		[]string{"Three", "Four"},
	}
	expPos := 25
	if !metadataEqualForTesting(out, exp) {
		t.Errorf("%#v != %#v", out, exp)
	}
	if pos != expPos {
		t.Errorf("%#v != %#v", pos, expPos)
	}
	in = `One: Two  
Three: Four  

And: More
Text: Here
`
	out, pos = MarkdownMetadata([]byte(in))
	exp = [][]string{
		[]string{"One", "Two"},
		[]string{"Three", "Four"},
	}
	expPos = 25
	if !metadataEqualForTesting(out, exp) {
		t.Errorf("%#v != %#v", out, exp)
	}
	if pos != expPos {
		t.Errorf("%#v != %#v", pos, expPos)
	}
	in = `One: Two
Three: Four

Above items don't have trailing two spaces, unlike the tests before.
`
	out, pos = MarkdownMetadata([]byte(in))
	exp = [][]string{
		[]string{"One", "Two"},
		[]string{"Three", "Four"},
	}
	expPos = 21
	if !metadataEqualForTesting(out, exp) {
		t.Errorf("%#v != %#v", out, exp)
	}
	if pos != expPos {
		t.Errorf("%#v != %#v", pos, expPos)
	}
	in = `One: Two
Three: Four`
	out, pos = MarkdownMetadata([]byte(in))
	exp = [][]string{
		[]string{"One", "Two"},
		[]string{"Three", "Four"},
	}
	expPos = 19
	if !metadataEqualForTesting(out, exp) {
		t.Errorf("%#v != %#v", out, exp)
	}
	if pos != expPos {
		t.Errorf("%#v != %#v", pos, expPos)
	}
	in = `One: Two
Three: Four
and text, which indicates the metadata was just looking like metadata.`
	out, pos = MarkdownMetadata([]byte(in))
	exp = [][]string{}
	expPos = 0
	if !metadataEqualForTesting(out, exp) {
		t.Errorf("%#v != %#v", out, exp)
	}
	if pos != expPos {
		t.Errorf("%#v != %#v", pos, expPos)
	}
	in = `One: Two
Three: Four
and text, which indicates the metadata was just looking like metadata.

///

With a summary (before the ///).`
	out, pos = MarkdownMetadata([]byte(in))
	exp = [][]string{[]string{"Summary", `One: Two
Three: Four
and text, which indicates the metadata was just looking like metadata.
`}}
	expPos = 0
	if !metadataEqualForTesting(out, exp) {
		t.Errorf("%#v != %#v", out, exp)
	}
	if pos != expPos {
		t.Errorf("%#v != %#v", pos, expPos)
	}
	in = `One: Two
Three: Four
and text, which indicates the metadata was just looking like metadata.

///
///

With a summary (before the ///).`
	out, pos = MarkdownMetadata([]byte(in))
	exp = [][]string{[]string{"Summary", `One: Two
Three: Four
and text, which indicates the metadata was just looking like metadata.
`}}
	expPos = 101
	if !metadataEqualForTesting(out, exp) {
		t.Errorf("%#v != %#v", out, exp)
	}
	if pos != expPos {
		t.Errorf("%#v != %#v", pos, expPos)
	}
	in = `One: Two
Three: Four

Blahzay blah.

///

With a summary (before the ///).`
	out, pos = MarkdownMetadata([]byte(in))
	exp = [][]string{
		[]string{"One", "Two"},
		[]string{"Three", "Four"},
		[]string{"Summary", "\nBlahzay blah.\n"},
	}
	expPos = 21
	if !metadataEqualForTesting(out, exp) {
		t.Errorf("%#v != %#v", out, exp)
	}
	if pos != expPos {
		t.Errorf("%#v != %#v", pos, expPos)
	}
	in = `One: Two
Three: Four

Blahzay blah.

///
///

With a summary (before the ///).`
	out, pos = MarkdownMetadata([]byte(in))
	exp = [][]string{
		[]string{"One", "Two"},
		[]string{"Three", "Four"},
		[]string{"Summary", "\nBlahzay blah.\n"},
	}
	expPos = 45
	if !metadataEqualForTesting(out, exp) {
		t.Errorf("%#v != %#v", out, exp)
	}
	if pos != expPos {
		t.Errorf("%#v != %#v", pos, expPos)
	}
}

func TestGeneral(t *testing.T) {
	in := `This is a general test of features.

# Heading One

Normal paragraph with a reasonable amount of text in order to test wrapping.

Longer paragraph to test much the same, but with
new lines embedded to test they
are ignored.

## Heading Two

Some *emphasized*, **doubly emphasized**, and ***triply emphasized*** text.

A bit of ~~striked through~~ text as well.

# Heading Three

 *  A simple
     *  list
     *  of
 *  items

Name  | Age
------|----
Bob   | 27
Alice | 23

omit  | omit
------|----
Bob   | 27
Alice | 23

` + "```" + `
A block of code
   that will
  not wrap, of course
` + "```" + `

Also, a quick bit of ` + "`inline code`" + ` to test.

Autolinking http://example.com for a test.

---

> Block quote at level
> one.
>
>> Block quote at level
>> two
>
> Back to
> level one.

Test an ![image](image.png) and a [link](http://example.com).

Force a line break  
just above this.

quick
: definition list

for
: testing

TODO: Seems there's a bug with switching from definition lists to tables in
BlackFriday?

Table | With | Alignments
:--- | ---: | :---:
L | R | C


Table | That | Is | Very | Wide
--- | --- | --- | --- | ---
This is to test | the wrapping | capabilities of the | table, as best as | it can.
|
There is only so | much it can | do, of course. | But it should do the best | it can.

`
	out := string(MarkdownToTextNoMetadata([]byte(in), &Options{Width: 40}))
	exp := `This is a general test of features.

--[ Heading One ]--

    Normal paragraph with a reasonable
    amount of text in order to test
    wrapping.

    Longer paragraph to test much the
    same, but with new lines embedded
    to test they are ignored.

    --[ Heading Two ]--

        Some *emphasized*, **doubly
        emphasized**, and ***triply
        emphasized*** text.

        A bit of ~~striked through~~
        text as well.

--[ Heading Three ]--

      * A simple
          * list
          * of
      * items

    +-------+-----+
    | Name  | Age |
    +-------+-----+
    | Bob   | 27  |
    | Alice | 23  |
    +-------+-----+

    +-------+----+
    | Bob   | 27 |
    | Alice | 23 |
    +-------+----+

    A block of code
       that will
      not wrap, of course

    Also, a quick bit of "inline code"
    to test.

    Autolinking http://example.com for
    a test.

    ------------------------------------

    > Block quote at level one.

    > > Block quote at level two

    > Back to level one.

    Test an [image] image.png and a
    [link] http://example.com.

    Force a line break
    just above this.

    quick  definition list
    for    testing

    TODO: Seems there's a bug with
    switching from definition lists to
    tables in BlackFriday?

    +-------+------+------------+
    | Table | With | Alignments |
    +-------+------+------------+
    | L     |    R |     C      |
    +-------+------+------------+

    +-------+----------+--------------+--------+------+
    | Table | That     | Is           | Very   | Wide |
    +-------+----------+--------------+--------+------+
    | This  | the      | capabilities | table, | it   |
    | is    | wrapping | of           | as     | can. |
    | to    |          | the          | best   |      |
    | test  |          |              | as     |      |
    |       |          |              |        |      |
    | There | much     | do,          | But    | it   |
    | is    | it       | of           | it     | can. |
    | only  | can      | course.      | should |      |
    | so    |          |              | do     |      |
    |       |          |              | the    |      |
    |       |          |              | best   |      |
    +-------+----------+--------------+--------+------+
`
	if out != exp {
		t.Errorf("%#v\n!=\n%#v", out, exp)
	}
}

func metadataEqualForTesting(m1 [][]string, m2 [][]string) bool {
	if len(m1) != len(m2) {
		return false
	}
	for i := 0; i < len(m1); i++ {
		if len(m1[i]) != len(m2[i]) {
			return false
		}
		for j := 0; j < len(m1[i]); j++ {
			if m1[i][j] != m2[i][j] {
				return false
			}
		}
	}
	return true
}
