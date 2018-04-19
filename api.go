package chord

import (
	"fmt"
	"log"
	"net/http"

	"github.com/Joe-xu/glog"

	"github.com/bitly/go-simplejson"
)

func (n *Node) HttpServer() {

	http.HandleFunc("/fingers", n.FingerTabInfo)
	http.HandleFunc("/store", n.Storage)

	err := http.ListenAndServe(n.config.HttpPort, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func (n *Node) FingerTabInfo(w http.ResponseWriter, req *http.Request) {

	info := simplejson.New()

	n.RLock()
	info.Set("successor", n.fingers[0].node.json())
	info.Set("predecessor", n.predecessor.json())
	n.RUnlock()

	out, err := info.MarshalJSON()
	if err != nil {
		glog.Error(err)
	}

	fmt.Fprintf(w, "%s", out)
}

func (n *Node) Storage(w http.ResponseWriter, req *http.Request) {

	res := simplejson.New()

	switch req.Method {
	case http.MethodGet: //	query key

		key := req.FormValue("key")
		data := n.store.Get(key)

		res.Set("key", key)
		res.Set("data", data)

	case http.MethodPost: //	new record

		key := req.FormValue("key")
		data := req.FormValue("data")

		if key == "" {
			res.Set("res", "fail")
		} else {
			n.store.Store(key, data)
			res.Set("res", "ok")
		}

	default:
		http.NotFound(w, req)
		return
	}

	out, err := res.MarshalJSON()
	if err != nil {
		glog.Error(err)
	}

	fmt.Fprintf(w, "%s", out)

}
