// taskflowctl is an operations client for TaskFlow's authenticated cloud API.
// It never talks to MySQL/Kubernetes directly and never stores credentials.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yfj898/taskflow-go/pkg/cloudclient"
)

const help = `taskflowctl — authenticated cloud resource order client

Global flags (before command):
  -endpoint URL     TaskFlow API (HTTPS required except 127.0.0.1/localhost)
  -token-file PATH  POSIX token file, mode 0600; use environment on Windows
  -timeout          HTTP operation timeout (default 20s)

Commands:
  quota TENANT
  quota-set TENANT CPU_MILLI MEMORY_MIB REPLICAS -key=BUSINESS_ORDER_ID
  resources TENANT [LIMIT [OFFSET]]
  resource RESOURCE_ID
  orders RESOURCE_ID [LIMIT [OFFSET]]
  workflows RESOURCE_ID [LIMIT [OFFSET]]
  workflow WORKFLOW_ID
  wait WORKFLOW_ID [-wait-timeout=10m]
  provision TENANT NAME SPEC_JSON_FILE -key=BUSINESS_ORDER_ID
  resize RESOURCE_ID SPEC_JSON_FILE -key=BUSINESS_ORDER_ID
  delete RESOURCE_ID -key=BUSINESS_ORDER_ID
  verification-submit REQUEST_JSON_FILE
  verification-get RUN_UUID

The -key flag is accepted before or after positional arguments for mutations.
The token may instead be supplied through TASKFLOW_ACCESS_TOKEN environment
variable. A business-stable idempotency key is mandatory for every mutation.
Windows supports TASKFLOW_ACCESS_TOKEN; -token-file is not supported there
because this client does not validate Windows ACLs.
All output is JSON and never includes the bearer token.
`

type options struct {
	endpoint  string
	tokenFile string
	timeout   time.Duration
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "taskflowctl:", err)
		os.Exit(1)
	}
}

func run(args []string, out, errOut io.Writer) error {
	global := flag.NewFlagSet("taskflowctl", flag.ContinueOnError)
	global.SetOutput(errOut)
	o := options{}
	global.StringVar(&o.endpoint, "endpoint", firstNonempty(os.Getenv("TASKFLOW_ENDPOINT"), "http://127.0.0.1:18088"), "TaskFlow endpoint")
	global.StringVar(&o.tokenFile, "token-file", os.Getenv("TASKFLOW_TOKEN_FILE"), "OIDC access token file")
	global.DurationVar(&o.timeout, "timeout", 20*time.Second, "per-command timeout")
	if len(args) == 0 || args[0] == "help" || args[0] == "-help" || args[0] == "--help" {
		_, _ = fmt.Fprint(out, help)
		return nil
	}
	if err := global.Parse(args); err != nil {
		return err
	}
	if global.NArg() == 0 {
		_, _ = fmt.Fprint(out, help)
		return errors.New("command required")
	}
	if err := validateEndpoint(o.endpoint); err != nil {
		return err
	}
	if o.timeout <= 0 || o.timeout > 15*time.Minute {
		return errors.New("timeout must be > 0 and <= 15 minutes")
	}
	client, err := cloudclient.New(o.endpoint, tokenSource(o.tokenFile), &http.Client{Timeout: o.timeout})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	command := global.Arg(0)
	parsed := global.Args()[1:]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(errOut)
	key := flags.String("key", "", "stable business idempotency key")
	waitTimeout := flags.Duration("wait-timeout", 10*time.Minute, "workflow wait timeout")
	// FlagSet stops at its first positional argument. Split -key / -wait-timeout
	// from the positional arguments so scripts can use intuitive argument order.
	positionals, err := extractFlags(parsed, flags)
	if err != nil {
		return err
	}
	if err := validateArgCount(command, positionals); err != nil {
		return err
	}
	mutating := command == "quota-set" || command == "provision" || command == "resize" || command == "delete"
	if mutating && (strings.TrimSpace(*key) != *key || *key == "" || len(*key) > 128) {
		return errors.New("mutation requires -key=BUSINESS_ORDER_ID (1..128 trimmed characters)")
	}
	if !mutating && *key != "" {
		return errors.New("-key is accepted only for mutations")
	}
	if command != "wait" && *waitTimeout != 10*time.Minute {
		return errors.New("-wait-timeout only applies to wait")
	}
	var response any
	opCtx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()
	switch command {
	case "verification-submit":
		data, e := readJSONFile(positionals[0])
		if e != nil {
			return e
		}
		var input cloudclient.VerificationRequest
		if e := json.Unmarshal(data, &input); e != nil {
			return e
		}
		response, err = client.SubmitVerification(opCtx, input)
	case "verification-get":
		response, err = client.GetVerificationRun(opCtx, positionals[0])
	case "quota":
		response, err = client.GetQuota(opCtx, positionals[0])
	case "quota-set":
		cpu, e1 := decimal(positionals[1])
		memory, e2 := decimal(positionals[2])
		replicas, e3 := decimal(positionals[3])
		if e1 != nil || e2 != nil || e3 != nil {
			return errors.New("quota limits must be nonnegative integers")
		}
		response, err = client.SetQuota(opCtx, positionals[0], cloudclient.QuotaLimits{CPUMilliLimit: int64(cpu), MemoryMiBLimit: int64(memory), ReplicaLimit: int64(replicas)}, *key)
	case "resources":
		limit, offset, e := parsePage(positionals[1:])
		if e != nil {
			return e
		}
		response, err = client.ListResources(opCtx, positionals[0], limit, offset)
	case "resource":
		id, e := positiveID(positionals[0])
		if e != nil {
			return e
		}
		response, err = client.GetResource(opCtx, id)
	case "orders":
		id, e := positiveID(positionals[0])
		if e != nil {
			return e
		}
		limit, offset, e := parsePage(positionals[1:])
		if e != nil {
			return e
		}
		response, err = client.ListOrders(opCtx, id, limit, offset)
	case "workflows":
		id, e := positiveID(positionals[0])
		if e != nil {
			return e
		}
		limit, offset, e := parsePage(positionals[1:])
		if e != nil {
			return e
		}
		response, err = client.ListWorkflows(opCtx, id, limit, offset)
	case "workflow":
		id, e := positiveID(positionals[0])
		if e != nil {
			return e
		}
		response, err = client.GetWorkflow(opCtx, id)
	case "wait":
		id, e := positiveID(positionals[0])
		if e != nil {
			return e
		}
		if *waitTimeout <= 0 || *waitTimeout > 24*time.Hour {
			return errors.New("wait timeout must be > 0 and <= 24 hours")
		}
		waitCtx, waitCancel := context.WithTimeout(ctx, *waitTimeout)
		defer waitCancel()
		response, err = client.WaitWorkflow(waitCtx, id, time.Second)
	case "provision":
		spec, e := readJSONFile(positionals[2])
		if e != nil {
			return e
		}
		response, err = client.Provision(opCtx, cloudclient.ProvisionWorkflow{TenantID: positionals[0], Name: positionals[1], Provider: "kubernetes", Spec: spec}, *key)
	case "resize":
		id, e := positiveID(positionals[0])
		if e != nil {
			return e
		}
		spec, e := readJSONFile(positionals[1])
		if e != nil {
			return e
		}
		response, err = client.ResizeWorkflow(opCtx, id, spec, *key)
	case "delete":
		id, e := positiveID(positionals[0])
		if e != nil {
			return e
		}
		response, err = client.DeleteWorkflow(opCtx, id, *key)
	default:
		return fmt.Errorf("unsupported command %q; use help", command)
	}
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(response)
}

func validateEndpoint(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("invalid TaskFlow endpoint")
	}
	if u.Scheme == "https" {
		return nil
	}
	if u.Scheme != "http" {
		return errors.New("TaskFlow endpoint must use HTTPS")
	}
	host := strings.ToLower(u.Hostname())
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("plaintext HTTP is allowed only for loopback addresses")
	}
	return nil
}

func tokenSource(filename string) cloudclient.TokenSource {
	return func(context.Context) (string, error) {
		if filename == "" {
			t := strings.TrimSpace(os.Getenv("TASKFLOW_ACCESS_TOKEN"))
			if t == "" {
				return "", errors.New("set TASKFLOW_ACCESS_TOKEN or -token-file with a short-lived OIDC token")
			}
			return t, nil
		}
		if runtime.GOOS == "windows" {
			return "", errors.New("-token-file requires POSIX permissions and is unavailable on Windows; use TASKFLOW_ACCESS_TOKEN")
		}
		info, err := os.Stat(filename)
		if err != nil {
			return "", err
		}
		if info.Mode().Perm()&0o077 != 0 {
			return "", errors.New("token file must be private: chmod 600")
		}
		if info.Size() > 16<<10 {
			return "", errors.New("token file too large")
		}
		b, err := os.ReadFile(filename)
		if err != nil {
			return "", err
		}
		t := strings.TrimSpace(string(b))
		if t == "" {
			return "", errors.New("token file is empty")
		}
		return t, nil
	}
}

func readJSONFile(filename string) (json.RawMessage, error) {
	info, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 || info.Size() > 256<<10 {
		return nil, errors.New("spec file must be 1..262144 bytes")
	}
	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(b, &value); err != nil || value == nil {
		return nil, errors.New("spec file must contain a JSON object")
	}
	return json.RawMessage(b), nil
}

func firstNonempty(v, fallback string) string {
	if v != "" {
		return v
	}
	return fallback
}

func positiveID(raw string) (uint64, error) {
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 {
		return 0, errors.New("ID must be a positive unsigned integer")
	}
	return value, nil
}

func decimal(raw string) (uint64, error) {
	return strconv.ParseUint(raw, 10, 63)
}

func parsePage(args []string) (int, int, error) {
	limit, offset := 20, 0
	var err error
	if len(args) > 0 {
		limit, err = strconv.Atoi(args[0])
		if err != nil {
			return 0, 0, errors.New("invalid page limit")
		}
	}
	if len(args) > 1 {
		offset, err = strconv.Atoi(args[1])
		if err != nil {
			return 0, 0, errors.New("invalid page offset")
		}
	}
	if limit < 1 || limit > 100 || offset < 0 {
		return 0, 0, errors.New("page limit must be 1..100, offset >= 0")
	}
	return limit, offset, nil
}

func validateArgCount(command string, args []string) error {
	minimum, maximum := 0, 0
	switch command {
	case "quota", "resource", "workflow", "wait", "delete", "verification-submit", "verification-get":
		minimum, maximum = 1, 1
	case "quota-set":
		minimum, maximum = 4, 4
	case "provision":
		minimum, maximum = 3, 3
	case "resize":
		minimum, maximum = 2, 2
	case "resources", "orders", "workflows":
		minimum, maximum = 1, 3
	default:
		return fmt.Errorf("unknown command %q", command)
	}
	if len(args) < minimum || len(args) > maximum {
		return fmt.Errorf("%s expects %d..%d positional arguments, got %d", command, minimum, maximum, len(args))
	}
	return nil
}

func extractFlags(raw []string, flags *flag.FlagSet) ([]string, error) {
	var flagArgs, positional []string
	for i := 0; i < len(raw); i++ {
		v := raw[i]
		if v == "-key" || v == "-wait-timeout" {
			if i+1 >= len(raw) {
				return nil, fmt.Errorf("missing value for %s", v)
			}
			flagArgs = append(flagArgs, v, raw[i+1])
			i++
			continue
		}
		if strings.HasPrefix(v, "-key=") || strings.HasPrefix(v, "-wait-timeout=") {
			flagArgs = append(flagArgs, v)
			continue
		}
		if strings.HasPrefix(v, "-") {
			return nil, fmt.Errorf("unknown command flag %s", v)
		}
		positional = append(positional, v)
	}
	if err := flags.Parse(flagArgs); err != nil {
		return nil, err
	}
	return positional, nil
}
