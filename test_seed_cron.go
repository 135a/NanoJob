package main; import ("fmt";"nanojob/core/parser"); func main() { p := parser.NewCronParser(); d, e := p.NextDelay("0/10 * * * * ?"); fmt.Println(d, e) }
