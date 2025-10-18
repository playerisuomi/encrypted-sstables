EXEC 	:= ./target/debug/enc-kv-store.exe
PASS 	?= "default"

.PHONY: build

build:
	@cargo build

run: build
	@$(EXEC) $(PASS)