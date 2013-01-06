/*
 * dupes -- find potential duplicate files.
 *
 * Walks a directory recursively, reporting on stdout the paths of files
 * that have the same size and SHA-1 sum.
 *
 * Copyright (c) 2013 Lars Buitinck.
 * License: MIT-style (http://opensource.org/licenses/MIT).
 */

package main

import (
    "crypto/sha1"
    "encoding/binary"
    "flag"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "strings"
)

type empty struct{}

type pathInfo struct {
    path string
    size int64
}

var errors chan error

func main() {
    var pairs, quiet bool
    var root string

    flag.BoolVar(&pairs, "pairs", false,
                 "print duplicate files' pathnames in pairs")
    flag.BoolVar(&quiet, "quiet", false,
                 "no error messages during the tree walk")
    flag.Parse()

    switch flag.NArg() {
    case 0:
        root = "."
    case 1:
        root = flag.Arg(0)
    default:
        fmt.Fprintf(os.Stderr, "usage: %s [flags] [root]\n", os.Args[0])
        os.Exit(3)
    }

    byhash := make(map[string][]string)

    errors = make(chan error, 10)
    hashdone := make(chan empty, 10)
    paths := make(chan pathInfo, 10)

    go hash(paths, byhash, hashdone)
    if !quiet {
        go func() {
            for e := range errors {
                fmt.Fprintf(os.Stderr, "%s: %s\n", os.Args[0], e)
            }
        }()
    }

    exitcode := walk(root, paths)
    <-hashdone
    close(errors)   // must close here because of multiple producers

    for _, paths := range byhash {
        if len(paths) > 1 {
            if pairs {
                printpairs(paths)
            } else {
                fmt.Println(strings.Join(paths, " "))
            }
        }
    }

    os.Exit(exitcode)
}

func printpairs(paths []string) {
    for i, p := range paths {
        for _, q := range paths[i+1:] {
            fmt.Println(p, q)
        }
    }
}

// Hash what comes out of paths and store it in byhash.
func hash(paths <-chan pathInfo, byhash map[string][]string,
          done chan<- empty) {
    for path := range paths {
        h, err := hashFile(path.path, path.size)
        if err == nil {
            byhash[h] = append(byhash[h], path.path)
        } else {
            errors <- err
        }
    }
    done <- empty{}
}

func hashFile(path string, size int64) (h string, err error) {
    f, err := os.Open(path)
    defer f.Close()
    if err != nil {
        return
    }

    sha := sha1.New()
    binary.Write(sha, binary.BigEndian, size)
    _, err = io.Copy(sha, f)
    if err != nil {
        return
    }

    h = string(sha.Sum(nil))
    return
}

// Walk root recursively, pushing regular files' paths on the channel.
func walk(root string, paths chan<- pathInfo) (exitcode int) {
    visit := func(path string, info os.FileInfo, err error) error {
        if err == nil {
            if regular := info.Mode() & os.ModeType == 0; regular {
                paths <- pathInfo{path, info.Size()}
            }
        } else {
            errors <- err
            exitcode = 1
        }
        return nil
    }

    err := filepath.Walk(root, visit)
    if err != nil {
        errors <- err
        exitcode = 1
    }

    close(paths)
    return
}
