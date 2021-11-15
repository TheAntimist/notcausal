package notcausal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TODO:
//  - Dependency is client specific?

//  ------------------ Structures

type Packet struct {
	// Request or Response
	Type    string                 `json:"t"`
	// Sub-Type of Request or Response
	Subject string                 `json:"s"`
	// Generic message content
	Message map[string]interface{} `json:"m"`
}

type Version struct {
	// Lamport Clock
	Timestamp int `json:"timestamp"`
	// Datacenter ID
	DCId int `json:"dc_id"`
}

func (v Version) LessThan(other Version) bool {
	return v.Timestamp < other.Timestamp
}

func (v Version) Equals(other Version) bool {
	return v.Timestamp == other.Timestamp && v.DCId == other.DCId
}

type Dependency struct {
	Key string `json:"key"`
	Version Version `json:"version"`
}

type StoreObject struct {
	key string
	value string
	version Version
	// This is because of requests being carried over.
	dep *Dependency
}

type DataCenter struct {
	id int
	store map[string]StoreObject
	storeMut *sync.RWMutex
	deferred map[string][]StoreObject
	// Current dependency. We only need one, given transitivity.
	curDep *Dependency
	serverListener net.Listener
	listenPort string
	conns map[string]net.Conn
	connMut *sync.RWMutex
	exitChan chan bool
}

// --------------------- Packet Utility Functions

func sendRequest(conn net.Conn, p Packet) error {
	b, _ := json.Marshal(p)
	fmt.Fprintf(conn, "LENGTH=%d\n", len(b))
	_, err := conn.Write(b)
	if err != nil {
		fmt.Printf("[error][%s] Error Writing to Conn: %s\n", conn.RemoteAddr(), err)
		return err
	}
	return nil
}

func handleResponse(conn net.Conn) (Packet, error) {
	remoteAddr := conn.RemoteAddr()
	reader := bufio.NewReader(conn)
	headerData, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("[error][%s] Error reading from conn: %s\n", remoteAddr, err)
		return Packet{}, err
	}

	header := strings.Split(strings.TrimSpace(headerData), "=")
	if header[0] != "LENGTH" {
		return Packet{}, err
	}

	jsonByteLength, err := strconv.Atoi(header[1])
	if err != nil {
		fmt.Printf("[error][%s] At reading length: %s\n", remoteAddr, err)
		return Packet{}, err
	}

	buf := make([]byte, jsonByteLength)
	n, err := io.ReadFull(reader, buf)
	if err != nil || n < jsonByteLength {
		fmt.Printf("[error][%s] Reading %d json bytes : %s\n", remoteAddr, jsonByteLength, err)
		return Packet{}, err
	}
	var p Packet
	err = json.Unmarshal(buf, &p)
	if err != nil {
		fmt.Printf("[error][%s] Parsing json: %s\n", remoteAddr, jsonByteLength, err)
		return Packet{}, err
	}

	return p, nil
}

// --------------------- SERVER

func (d *DataCenter) registerOthers(packet Packet, remoteAddr string) (Packet, bool) {
	port := packet.Message["port"].(string)
	d.connMut.Lock()
	if _, ok := d.conns[remoteAddr]; !ok {
		splits := strings.Split(remoteAddr, ":")
		conn, err := net.Dial("tcp", splits[0] + ":" + port)
		d.conns[remoteAddr] = conn
		if err != nil {
			fmt.Printf(" [error] Connection failed to server %s\n", remoteAddr)
		} else {
			fmt.Printf(" [debug] Connected to %s:%s\n", splits[0], port)
		}
	}
	d.connMut.Unlock()
	return Packet{
		Type:    "r",
		Subject: "reg",
		Message: nil,
	}, true
}

var packetFuncMap = map[string]func(Packet, string) (Packet, bool) {
	"reg": datacenter.registerOthers,
	"rw": datacenter.replicatedWrite,
}

func (d *DataCenter) listen() {
	l, err := net.Listen("tcp4", "0.0.0.0:0")

	if err != nil {
		fmt.Println(err)
		return
	}

	d.serverListener = l

	fmt.Println(" [debug] Started listening on port", l.Addr().String())
	splits := strings.Split(l.Addr().String(), ":")
	d.listenPort = splits[1]

	go func(l net.Listener) {
		defer l.Close()
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println(err)
				return
			}
			go d.handleConnection(conn)
		}
	}(l)
}

func (d DataCenter) handleConnection(conn net.Conn) {
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()

	fmt.Printf(" [debug] Accepted Connection from %s\n", remoteAddr)
	for {
		reader := bufio.NewReader(conn)
		netData, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("[debug] Disconnected from peer %s with reason: %s\n", remoteAddr, err)
			return
		}

		temp := strings.TrimSpace(netData)
		header := strings.Split(temp, "=")
		if header[0] != "LENGTH" {
			continue
		}

		jsonByteLength, err := strconv.Atoi(header[1])
		if err != nil {
			fmt.Printf("[error][%s] At reading length: %s\n", remoteAddr, err)
			return
		}

		buf := make([]byte, jsonByteLength)
		n, err := io.ReadFull(reader, buf)
		if err != nil || n < jsonByteLength {
			fmt.Printf("[error][%s] Reading %d json bytes : %s\n", remoteAddr, jsonByteLength, err)
			return
		}
		var p Packet
		err = json.Unmarshal(buf, &p)
		if err != nil {
			fmt.Printf("[error][%s] Parsing json: %s\n", remoteAddr, jsonByteLength, err)
			return
		}

		//p.peer = currentPeer
		//fmt.Printf("[info][%s] Got Packet: %s\n", remoteAddr, p)

		if p.Type == "q" && p.Subject == "stop" {
			// Deferred close is enabled.
			return
		}

		if pFunc, ok := packetFuncMap[p.Subject]; ok {
			resp, toSend := pFunc(p, remoteAddr)
			if toSend {
				b, _ := json.Marshal(resp)
				fmt.Fprintf(conn, "LENGTH=%d\n", len(b))
				_, err = conn.Write(b)
				if err != nil {
					fmt.Printf("[error][%s] Error Writing to Conn: %s\n", remoteAddr, err)
					return
				}
			}
		}
	}
}

// ----------------- Data Store

func (d* DataCenter) write(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: write key value")
		return
	}
	key := args[0]
	value := args[1]

	d.storeMut.Lock()
	version := Version{
		Timestamp: 1,
		DCId:      d.id,
	}

	if curVal, ok := d.store[key]; ok {
		version = Version{Timestamp: curVal.version.Timestamp + 1, DCId: d.id}
	}

	storeObj := StoreObject{
		key,
		value,
		version,
		d.curDep,
	}

	d.store[key] = storeObj
	repReq := Packet{
		Type:    "q",
		Subject: "rw",
		Message: map[string]interface{}{
			"key": storeObj.key,
			"value": storeObj.value,
			"version": storeObj.version,
			"dep": nil,
			"has_dep": storeObj.dep != nil,
		},
	}
	if storeObj.dep != nil {
		repReq.Message["dep"] = *storeObj.dep
	}
	go func(storeObj StoreObject, req Packet) {
		d.connMut.RLock()
		for _, conn := range d.conns {
			d := time.Second * time.Duration(rand.Intn(60))
			fmt.Printf(" [debug] Delaying the replication to %s by %0.3f seconds\n", conn.RemoteAddr().String(), d.Seconds())
			go func(conn net.Conn, req Packet, d time.Duration) {
				time.Sleep(d)
				sendRequest(conn, req)
				handleResponse(conn)
			}(conn, req, d)
		}
		d.connMut.RUnlock()
	}(storeObj, repReq)
	d.curDep = &Dependency{
		Key:     storeObj.key,
		Version: storeObj.version,
	}
	fmt.Printf(" [debug] Successfully wrote to store with new version: <%d, %d>\n", version.Timestamp, version.DCId)
	d.storeMut.Unlock()
}

func (d *DataCenter) recurReplicatedWrites(storeObject StoreObject) {
	d.store[storeObject.key] = storeObject
	fmt.Printf(" [debug] Updated value of %s to version <%d, %d>\n",
		storeObject.key, storeObject.version.Timestamp, storeObject.version.DCId)
	curVersion := storeObject.version
	if deps, ok := d.deferred[storeObject.key]; ok {
		var u = make([]StoreObject, len(deps))
		copy(u, deps)
		for i, storeObj := range deps {
			if storeObj.dep.Version.Equals(curVersion) {
				// TODO: Need to handle nested deps
				d.recurReplicatedWrites(storeObj)
				u = append(u[:i], u[i+1:]...)
			}
		}
		if len(u) == 0 {
			delete(d.deferred, storeObject.key)
		} else {
			d.deferred[storeObject.key] = u
		}
	}
}

func (d* DataCenter) replicatedWrite(p Packet, remoteAddr string) (Packet, bool) {

	// key string, value string, dep Dependency, version Version
	version := p.Message["version"].(map[string]interface{})
	storeObject := StoreObject{
		key:     p.Message["key"].(string),
		value:   p.Message["value"].(string),
		version: Version{
			int(version["timestamp"].(float64)),
			int(version["dc_id"].(float64)),
		},
		dep: nil,
	}
	if p.Message["has_dep"].(bool) {
		dependency := p.Message["dep"].(map[string]interface{})
		depVersion := dependency["version"].(map[string]interface{})
		storeObject.dep = &Dependency{
			dependency["key"].(string),
			Version{
				Timestamp: int(depVersion["timestamp"].(float64)),
				DCId:      int(depVersion["dc_id"].(float64)),
			},
		}
	}
	// Dep Check for deferring request
	if storeObject.dep != nil && !d.validDependencies(*storeObject.dep) {
		fmt.Printf(" [debug] Deferring request of %s <%d, %d> as dependency version %s <%d, %d> is not valid\n",
			storeObject.key, storeObject.version.Timestamp, storeObject.version.DCId,
			storeObject.dep.Key, storeObject.dep.Version.Timestamp, storeObject.dep.Version.DCId)
		dep := storeObject.dep
		d.storeMut.Lock()
		if curDeps, ok := d.deferred[dep.Key]; ok {
			d.deferred[dep.Key] = append(curDeps, storeObject)
		} else {
			deps := make([]StoreObject, 0)
			d.deferred[dep.Key] = append(deps, storeObject)
		}
		d.storeMut.Unlock()
		return Packet{
			Type: "r",
			Subject: "rw",
			Message: map[string]interface{}{"ack": true},
		}, true
	}

	// Data can be written with
	d.storeMut.RLock()
	curObj, ok := d.store[storeObject.key]
	d.storeMut.RUnlock()
	if ok && storeObject.version.LessThan(curObj.version) {
		// TODO: Possibly simplify this.
		fmt.Printf(" [debug] Current value of %s is <%d, %d> > attempted write version <%d, %d>\n",
			storeObject.key, curObj.version.Timestamp, curObj.version.DCId, storeObject.version.Timestamp, storeObject.version.DCId)
	} else {
		// Valid Write.
		// So we update the store with the new values
		// Remove all dependencies
		// Lock here instead of inside
		d.storeMut.Lock()
		d.recurReplicatedWrites(storeObject)
		// Replicated write, not the actual write
		d.storeMut.Unlock()
	}

	return Packet{
		Type: "r",
		Subject: "rw",
		Message: map[string]interface{}{"ack": true},
	}, true
}

// depCheck
func (d *DataCenter) validDependencies(dep Dependency) bool {
	d.storeMut.RLock()
	defer d.storeMut.RUnlock()
	if actualDep, ok := d.store[dep.Key]; (!ok) ||
		(ok && actualDep.version.LessThan(dep.Version)) {
		return false
	}
	return true
}

// -------------- User Input and actual code

func (d *DataCenter) safeExit(args []string) {
	d.connMut.RLock()
	for _, conn := range d.conns {
		conn.Close()
	}
	d.connMut.RUnlock()
	d.serverListener.Close()
	os.Exit(0)
}

func (d *DataCenter) connect(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: start server_ip port")
		return
	}

	remoteAddr := strings.Join(args[:2], ":")
	conn, err := net.Dial("tcp", remoteAddr)
	if err != nil {
		fmt.Printf(" [error] Connection failed to server %s\n", remoteAddr)
	}
	d.connMut.Lock()
	d.conns[remoteAddr] = conn
	d.connMut.Unlock()

	sendRequest(conn, Packet{
		Type:    "q",
		Subject: "reg",
		Message: map[string]interface{}{
			"port": d.listenPort,
		},
	})
	handleResponse(conn)
}

func (d *DataCenter) listConns(args []string) {
	d.connMut.RLock()
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Datacenters"})

	for addr, _ := range d.conns {
		table.Append([]string{addr})
	}
	table.Render()
	d.connMut.RUnlock()
}

func (d *DataCenter) items(args []string) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Key", "Value", "Version"})
	d.storeMut.RLock()
	for key, storeObject := range d.store {
		table.Append([]string{key, storeObject.value, fmt.Sprintf("<%d, %d>", storeObject.version.Timestamp, storeObject.version.DCId)})
	}
	d.storeMut.RUnlock()
	table.Render()

}

func (d *DataCenter) showDep(args []string) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Dependency Key", "Version"})
	d.storeMut.RLock()
	if d.curDep != nil {
		table.Append([]string{d.curDep.Key, fmt.Sprintf("<%d, %d>", d.curDep.Version.Timestamp, d.curDep.Version.DCId)})
	} else {
		table.Append([]string{"", ""})
	}
	d.storeMut.RUnlock()
	table.Render()

}

func (d *DataCenter) read(args []string) {
	key := args[0]

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{key})
	d.storeMut.Lock()
	if val, ok := d.store[key]; ok {
		table.Append([]string{val.value})
		d.curDep = &Dependency{
			key,
			val.version,
		}
	} else {
		table.Append([]string{""})
	}
	d.storeMut.Unlock()
	table.Render()
}

var (
	datacenter = DataCenter{
		id:       0,
		store:    make(map[string]StoreObject),
		storeMut: &sync.RWMutex{},
		deferred: make(map[string][]StoreObject),
		conns: make(map[string]net.Conn),
		connMut: &sync.RWMutex{},
		serverListener: nil,
	}
)

func hello(args []string) {
	fmt.Println("Hello!", strings.Join(args, " "))
}

var commandMap = map[string]func([]string) {
	"connect": datacenter.connect,
	"read": datacenter.read,
	"write": datacenter.write,
	"list": datacenter.listConns,
	"items": datacenter.items,
	"dep": datacenter.showDep,
	"hello": hello,
	"exit": datacenter.safeExit,
}

func inputLoop() {
	// create new reader from stdin
	reader := bufio.NewReader(os.Stdin)
	// start infinite loop to continuously listen to input
	for {
		fmt.Print("cs> ")
		// read by one line (enter pressed)
		s, err := reader.ReadString('\n')
		// check for errors
		if err != nil {
			// close channel just to inform others
			return
		}
		s = strings.TrimSpace(s)
		sp := strings.Split(s, " ")
		if len(s) > 0 {
			if commandFunc, ok := commandMap[sp[0]]; ok {
				commandFunc(sp[1:])
			}
		}
	}
}

func main() {
	fmt.Println("Hello User! How causally inconsistent are you?\n\n")
	rand.Seed(time.Now().UnixNano())
	datacenter.id = int(rand.Uint32())
	datacenter.listen()
	inputLoop()
}