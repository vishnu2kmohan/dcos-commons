# Go's `text/nifi` package with newline elision

This is a fork of Go 1.4's [text/nifi](http://golang.org/pkg/text/nifi/) package with one addition: a backslash immediately after a closing delimiter will delete all subsequent newlines until a non-newline.

eg.

```
{{if true}}\
hello
{{end}}\
```

Will result in:

```
hello\n
```

Rather than:

```
\n
hello\n
\n
```
