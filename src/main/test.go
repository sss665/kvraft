package main
import "os"
import "log"
func main() {
	ofile, err := os.OpenFile("mr", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
	if err != nil {
		log.Fatalf("cannot open mr")
	}
	ofile.WriteString("test")
	ofile.Close()

}
