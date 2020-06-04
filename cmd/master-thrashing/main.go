package main

func main() {
}

/*

func main() {
	c, _ := config.LoadGet(filepath.Join("config", "config.json"))

	nStorage := flag.Int("s", -1, "The index of the Storage instance to start")

	master.NewMaster()

	if *nStorage != -1 {
		if *nStorage < 0 || *nStorage >= len(c.Storages) {
			panic(fmt.Sprintf("Invalid storage index %d", nStorage))
		}

		fmt.Printf("Starting Storage at address %q\n", c.Storages[*nStorage])
		go func() {
			s := storage.NewStorage()
			storage.ServeRPCSync(s, c.Storages[*nStorage])
		}()
	} else {
		fmt.Printf("Starting Master at address %q\n", c.Master)
		go func() {
			m := master.NewMaster(c.Storages, c)
			master.ServeRPCSync(m, c.Master)
		}()
	}

}

*/
