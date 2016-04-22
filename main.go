package main

import (
	"os"
	"strconv"
)

var doMake, doTest, recursion bool
var caseSensitive bool
var directory, indexFile string
var pattern string
var coworkers int

func parseArgs() (ok bool) {
	var dir *string
	var dirInt *int
	for _, v := range os.Args[1:] {
		if dir != nil {
			*dir = v
			dir = nil
		} else if dirInt != nil {
			i, err := strconv.ParseInt(v, 10, 32)
			if err != nil { return false }
			*dirInt = int(i)
			dirInt = nil
		} else {
			switch v {
			case "-d", "--dir":
				dir = &directory
			case "-i", "--index":
				dir = &indexFile
			case "-j", "--co":
				dirInt = &coworkers
			case "-m", "--make":
				doMake = true
			case "-t", "--test":
				doTest = true
			case "-r":
				recursion = true
			case "-c":
				caseSensitive = true
			case "-C":
				caseSensitive = false
			default:
				pattern = v
			}
		}
	}
	return dir == nil && dirInt == nil
}
func usage() {
	printf("Usage: %s [-r] [-cC] [-d directory] [-i index file] search\n", os.Args[0])
	printf("       %s -m [-r] [-d directory] [-i index file] pattern\n", os.Args[0])
	printf("       %s -t [-r] [-d directory] pattern\n", os.Args[0])
}

func main() {
	if parseArgs() {
		if doMake && !doTest && pattern != "" {
			if directory == "" {
				directory = "./"
			}

			if indexFile == "" {
				if directory[len(directory)-1] == '/' {
					indexFile = directory + ".index"
				} else {
					indexFile = directory + "/.index"
				}
			}

			makeIndex(directory, indexFile, pattern)
			return
		}
		if doTest && !doMake && pattern != "" {

			printf("not implements\n")
			return
		}
		if !doMake && !doTest && pattern != "" {
			if directory == "" {
				directory = "./"
			}

			if indexFile == "" {
				if directory[len(directory)-1] == '/' {
					indexFile = directory + ".index"
				} else {
					indexFile = directory + "/.index"
				}
			}

			searchIndex(directory, indexFile, []byte(pattern))
			return
		}
	}
	usage()
}

