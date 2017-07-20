# implementation of Chord protocol

:construction:

## Usage

0. `go get -u github.com/Joe-xu/chord`
1. Init a Chord ring

```go
	li, err := net.Listen("tcp", ":50015")
	if err != nil {
		log.Fatal(err)
	}

	n := chord.NewNode(&chord.Config{
		HashMethod: md5.New(),
		Listener:   li,
		Timeout:    5 * time.Second,
	})

	err = n.JoinAndServe()
	if err != nil {
		log.Fatal(err)
	}
```

2. Join the Ring

```go
	li, err := net.Listen("tcp", ":50016")
	if err != nil {
		log.Fatal(err)
	}

	n := chord.NewNode(&chord.Config{
		HashMethod: md5.New(),
		Listener:   li,
		Timeout:    5 * time.Second,
		Introducer: &chord.NodeInfo{
			Addr: "localhost:50015", //node on the existing ring
		},
	})

	err = n.JoinAndServe()
	if err != nil {
		log.Fatal(err)
	}
```

3. Locate

```go
	r, err := chord.JoinRing(&chord.Config{
		Introducer: &chord.NodeInfo{
			Addr: ":50016",
		},
		HashMethod: md5.New(),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer r.Leave()

	nodeInfo, err := r.Locate("foo.jpg")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(nodeInfo)
```

## Build

[protocol-buffers](1) is required

```sh
>go generate
>go build
```

## References

* [Chord: A Scalable Peer-to-peer Lookup Service for Internet Applications](https://pdos.csail.mit.edu/6.824/papers/stoica-chord.pdf)

[1]:(https://github.com/golang/protobuf) "Go support for Protocol Buffers"