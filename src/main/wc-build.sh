go build -race -buildmode=plugin ../mrapps/wc.go
TIMEOUT = ""
$TIMEOUT ../mrcoordinator ../pg*txt &
go run -race mrworker.go wc.so
