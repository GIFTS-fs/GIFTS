package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/GIFTS-fs/GIFTS/bench"
	"github.com/GIFTS-fs/GIFTS/client"
	"github.com/GIFTS-fs/GIFTS/config"
	"github.com/GIFTS-fs/GIFTS/generate"
)

const (
	benchName = "clientreadwave"
	benchMsg  = "Running benchmark: a long wave for reading"

	// nRounds = 3

	// 1 block
	nCreateTiny = 5
	// 2 blocks
	nCreateMedium = 6
	// 5 blocks
	nCreateLarge = 6
	// n_storage blocks
	nCreateColossal = 2

	rFactor = uint(1)

	nReadersPerGroup = 5

	seed1         = 725695988451494264
	seed2         = 859742921935149993
	randPickUpper = 4

	runTime           = 120 // sec
	stateChangePeriod = 8   // sec
	jobPeriod         = 5   // sec
)

var (
	configPath = flag.String("conf", bench.DefaultConfigPathClient, "config file")
	label      = flag.String("label", "", "label")
	// nCreateTotal = nCreateTiny + nCreateMedium + nCreateLarge + nCreateColossal
)

var wg sync.WaitGroup

func createFiles(c *client.Client, config *config.Config, fNamePrefix string) (files []string) {
	g := generate.NewGenerate()

	// TODO: DRY
	for i := 0; i < nCreateTiny; i++ {
		fileSize := 1
		fName := fmt.Sprintf("%v-tiny-%v", fNamePrefix, i)

		data := make([]byte, fileSize)
		g.Read(data)
		c.Store(fName, rFactor, data)
		files = append(files, fName)
	}
	for i := 0; i < nCreateMedium; i++ {
		fileSize := 2 * config.GiftsBlockSize
		fName := fmt.Sprintf("%v-medium-%v", fNamePrefix, i)

		data := make([]byte, fileSize)
		g.Read(data)
		c.Store(fName, rFactor, data)
		files = append(files, fName)
	}
	for i := 0; i < nCreateLarge; i++ {
		fileSize := 5 * config.GiftsBlockSize
		fName := fmt.Sprintf("%v-large-%v", fNamePrefix, i)

		data := make([]byte, fileSize)
		g.Read(data)
		c.Store(fName, rFactor, data)
		files = append(files, fName)
	}
	for i := 0; i < nCreateColossal; i++ {
		fileSize := len(config.Storages) * config.GiftsBlockSize
		fName := fmt.Sprintf("%v-colossal-%v", fNamePrefix, i)

		data := make([]byte, fileSize)
		g.Read(data)
		c.Store(fName, rFactor, data)
		files = append(files, fName)
	}

	return
}

func doReading(files []string, r *rand.Rand, readers []*client.Client) {
	done := make(chan bool)

	ticker := time.NewTicker(stateChangePeriod * time.Second)
	defer ticker.Stop()

	// A finite state machine, clk is 1 sec, state changes per stateChangePeriod sec

	idx := 0

	// -1: done
	// 0: idle
	// 1: random pick a file
	// 2: read the file at idx
	currentState := 0

	for _, r := range readers {
		go func(r *client.Client) {
			for {
				switch currentState {
				case -1:
					return
				case 1:
					r.Read(files[rand.Intn(len(files))])
				case 2:
					r.Read(files[idx])
				}
				time.Sleep(time.Second)
			}
		}(r)
	}

	go func() {
		for {
			select {
			case <-done:
				currentState = -1
				return
			case t := <-ticker.C:
				s := r.Intn(randPickUpper)
				switch s {
				case 0:
					currentState = 0
				case 1:
					currentState = 1
				default:
					idx = rand.Intn(len(files))
					currentState = 2
				}
				// use the address of the first reader to distinguish groups
				fmt.Printf("[Group(%v)] State changed to %v at time %v\n", &readers[0], currentState, t)
			}
		}
	}()

	time.Sleep(runTime * time.Second)
	done <- true
	wg.Done()
}

func main() {
	fmt.Println(benchMsg)

	flag.Parse()
	config, err := config.LoadGet(*configPath)
	bench.ExitUnless(err == nil, fmt.Sprintf("Error loading config: %v", err))

	file, err := os.Create(fmt.Sprintf("%vresults-%d.csv", *label, time.Now().UnixNano()))
	bench.ExitUnless(err == nil, fmt.Sprintf("Failed to create results file: %v", err))
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	writer.WriteString("Time,MB/s")

	// Run nRounds rounds
	// For each round:
	//   iRound*k_create files are created (k_create is a hard-coded const)
	// <<<< Do the "round" thing out side of the individual benchmark to avoid interference >>>>
	//   For each half of the reders:
	//     share a global random seed (hard-code), pick from (0,1,2,3,4,5)
	//     For readPeriod seconds
	//       hotspot: read the same random file to read if >1
	//       normal read: randomly pick a file to read if 1
	//       idle: do nothing if 0

	// seed the rand to randomly pick a file for non-hot read
	rand.Seed(time.Now().UnixNano())

	// create 2 seeded rand
	rand1 := rand.New(rand.NewSource(seed1))
	rand2 := rand.New(rand.NewSource(seed2))

	// create 2 groups of readers
	readerGroup1 := make([]*client.Client, nReadersPerGroup)
	readerGroup2 := make([]*client.Client, nReadersPerGroup)
	for i := 0; i < nReadersPerGroup; i++ {
		readerGroup1[i] = client.NewClient([]string{config.Master}, config)
		readerGroup2[i] = client.NewClient([]string{config.Master}, config)
	}

	createrClient := client.NewClient([]string{config.Master}, config)

	files := createFiles(createrClient, config, "")
	rand.Shuffle(len(files), func(i, j int) {
		files[i], files[j] = files[j], files[i]
	})
	fmt.Printf("Created files: %v\n", files)

	fmt.Printf("Creating 2 sub-threads for reading\n")
	wg.Add(2)
	go doReading(files, rand1, readerGroup1)
	go doReading(files, rand2, readerGroup2)
	wg.Wait()
}
