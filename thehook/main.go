package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

type Config struct {
	apiPath  string
	apiKey   string
	Commands []string
	slackURL string
	addr     string
}

type HookMsg struct {
	Key     string
	Version string
}

type SlackMessage struct {
	Fallback string       `json:"fallback"`
	Pretext  string       `json:"pretext"`
	Color    string       `json:"color"`
	Fields   []SlackField `json:"fields"`
}

type SlackField struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short"`
}

type FancySlackMessage struct {
	Text        string         `json:"text"`
	Attachments []SlackMessage `json:"attachments"`
}

type SimpleSlackMessage struct {
	Text string `json:"text"`
}

func execEvent(e HookMsg) {
	msg := FancySlackMessage{}
	msg.Text = fmt.Sprintf("!! Result for version %s !!", e.Version)
	for _, cmd := range conf.Commands {
		am := SlackMessage{}
		r, err := runCmd(cmd, []string{e.Version})
		am.Color = "#008000"
		am.Pretext = fmt.Sprintf("%s ok", cmd)
		am.Fallback = fmt.Sprintf("%s ok", cmd)
		if err != nil || r.StatusCode != 0 {
			am.Color = "#D00000"
			am.Pretext = fmt.Sprintf("%s failed!", cmd)
			am.Fallback = fmt.Sprintf("%s failed!", cmd)
		}
		if err != nil {
			am.Fields = append(am.Fields, SlackField{
				Title: "goerr",
				Value: err.Error(),
				Short: true,
			})
		}
		if r.Stderr != "" {
			am.Fields = append(am.Fields, SlackField{
				Title: "stderr",
				Value: r.Stderr,
				Short: true,
			})
		}
		if r.Stdout != "" {
			am.Fields = append(am.Fields, SlackField{
				Title: "stdout",
				Value: r.Stdout,
				Short: true,
			})
		}
		msg.Attachments = append(msg.Attachments, am)
	}
	payload, _ := json.Marshal(msg)
	log.Println(postToSlack(payload))
}

func postToSlack(payload []byte) error {
	req, _ := http.NewRequest("POST", conf.slackURL, bytes.NewBuffer(payload))
	req.Header.Add("content-type", "application/json")
	res, err := http.DefaultClient.Do(req)
	defer res.Body.Close()
	if err != nil {
		log.Println(err.Error())
		return fmt.Errorf("Error posting to slack")
	}
	if res.StatusCode != 200 {
		log.Printf("%s", payload)
		log.Println(res.StatusCode)
		return fmt.Errorf("Error posting to slack")
	}
	return nil
}

type result struct {
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	StatusCode int    `json:"status_code"`
}

func runCmd(name string, cmdArgs []string) (result, error) {
	cmd := exec.Command(name, cmdArgs...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	r := result{
		stdout.String(),
		stderr.String(),
		-1,
	}
	if err == nil {
		r.StatusCode = cmd.ProcessState.Sys().(syscall.WaitStatus).ExitStatus()
	}
	return r, err
}

func ciHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Nope", http.StatusBadRequest)
		return
	}
	event := HookMsg{}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, &event)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	if event.Key != conf.apiKey {
		http.Error(w, "Nope", http.StatusUnauthorized)
		return
	}
	if event.Version == "" {
		http.Error(w, "Missing Version", http.StatusBadRequest)
		return
	}
	go execEvent(event)
	msg := SimpleSlackMessage{
		Text: fmt.Sprintf("!! Starting deploy of %s !!", event.Version),
	}
	payload, _ := json.Marshal(msg)
	log.Println(postToSlack(payload))
	fmt.Fprintf(w, "CI Event in progress")
	return
}

var conf *Config

func main() {
	conf = &Config{}
	conf.addr = os.Getenv("THEHOOK_ADDR")
	if conf.addr == "" {
		conf.addr = ":4000"
	}
	conf.apiPath = os.Getenv("THEHOOK_API_PATH")
	if conf.apiPath == "" {
		conf.apiPath = "/ci/deploy"
	}
	conf.apiKey = os.Getenv("THEHOOK_API_KEY")
	if conf.apiKey == "" {
		log.Fatalln("No THEHOOK_API_KEY found in env.")
	}
	conf.slackURL = os.Getenv("THEHOOK_SLACK_URL")
	if conf.slackURL == "" {
		log.Fatalln("No THEHOOK_SLACK_URL found in env.")
	}

	//a comma sep list of commands to execute
	envCMDs := os.Getenv("THEHOOK_CMDS")
	if envCMDs == "" {
		log.Fatalln("No command hooks found in env. Looking for something like: THEHOOK_CMDS=/somescript,/somescript2")
	}
	conf.Commands = strings.Split(envCMDs, ",")
	http.HandleFunc(conf.apiPath, ciHandler)
	log.Fatal(http.ListenAndServe(conf.addr, nil))
}
