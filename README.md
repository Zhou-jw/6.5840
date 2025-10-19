# Profile
```shell
sudo dnf in graphviz
time go test -run Backup -cpuprofile=cpu.out
go tool pprof -http=:8080 cpu.out
```