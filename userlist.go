package main

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/Pallinder/go-randomdata"
	"github.com/garyburd/redigo/redis"
)

type StatusUpdate struct {
	Channel string
	Name    string
	Status  bool
}

func NewStatusUpdate(channel string, name string, status bool) StatusUpdate {
	return StatusUpdate{channel, name, status}
}

const MAX_BUFF int = 1000000

const CONSUMERS int = 4
const CHANNELS int = 10
const USERS int = 100
const ADDRESS = ":6379"

func main() {
	rand.Seed(42)

	var wg sync.WaitGroup
	done := make(chan bool)
	defer close(done)

	updates := make(chan StatusUpdate, MAX_BUFF)
	close(updates)

	wg.Add(CHANNELS)
	for c := 0; c < CHANNELS; c++ {
		channel := fmt.Sprintf("channel-%04d", c)
		go simulateClients(channel, USERS, &wg, &done, &updates)
	}

	wg.Add(CONSUMERS)
	for r := 0; r < CONSUMERS; r++ {
		go runConsumer(&wg, &done, &updates)
	}

	fmt.Scanln()
	done <- true

	wg.Wait()
}

func runConsumer(wg *sync.WaitGroup, done *chan bool, in *chan StatusUpdate) {
	defer wg.Done()
	defer wg.Wait()

	if conn, err := redis.Dial("tcp", ADDRESS); err != nil {
		fmt.Println(err)
		return
	} else {
		defer conn.Close()
		for {
			select {
			case <-*done:
				return
			case status := <-*in:
				if status.Status {
					cerr := conn.Send("ZADD", status.Channel, 0, status.Name)
					if cerr != nil {
						fmt.Println(cerr)
					}
				} else {
					cerr := conn.Send("ZREM", status.Channel, status.Name)
					if cerr != nil {
						fmt.Println(cerr)
					}
				}
			}
		}
	}
}

func simulateClients(channel string, num int, wg *sync.WaitGroup, done *chan bool, out *chan StatusUpdate) {
	defer wg.Done()
	set := make(map[string]bool)
	cnt := 0
	for len(set) <= num {
		name := fmt.Sprintf("%s%02d", randomdata.SillyName(), cnt%100)
		set[name] = true
		cnt++
	}

	names := make([]string, 0, len(set))
	statuses := make([]bool, len(set))

	for name, _ := range set {
		names = append(names, name)
	}

	for {
		select {
		case <-*done:
			return
		default:
			idx := rand.Int31n(int32(num))
			statuses[idx] = !statuses[idx]
			//			fmt.Printf("%s (%v)\n", names[idx], statuses[idx])
			*out <- NewStatusUpdate(channel, names[idx], statuses[idx])
		}
	}
}
