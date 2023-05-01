package main

import "fmt"
import "C"

//export HelloGopher
func HelloGopher() {
  // Add comment
	fmt.Println("Hello Gopher!")
}

func main() {}
