package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

// Bind -
func BindClient(client string) string {
	//POST http://{{host}}:{{port}}/clients/client01/bind
	resp, err := http.Post(fmt.Sprintf("http://party:8085/clients/%s/bind", client), "", nil)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(0)
	}

	defer resp.Body.Close()
	bytes, _ := io.ReadAll(resp.Body)
	var data map[string]any
	_ = json.Unmarshal(bytes, &data)

	fmt.Printf("Bind: %s - %s \n", client, resp.Status)

	return data["routingKey"].(string)
}

func UnbindClient(client string) {
	//POST http://{{host}}:{{port}}/clients/client01/bind
	_, err := http.Post(fmt.Sprintf("http://party:8085/clients/%s/unbind", client), "", nil)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(0)
	}
}

// Ready -
func ClientReady(client string) {
	resp, err := http.Post(fmt.Sprintf("http://party:8085/clients/%s/ready", client), "", nil)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(0)
	}

	fmt.Printf("Ready: %s - %s \n", client, resp.Status)
}
