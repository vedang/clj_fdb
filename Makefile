.PHONY: test doc

compile:
	@echo "\nRecompiling Clojure files"
	lein do clean, deps, compile

start:
	@echo "\nStarting a Clojure Repl"
	lein trampoline repl :headless

clj: compile start

test: compile
	@echo "\nRunning all tests"
	lein test

clean:
	lein clean

install:
	lein install

doc:
	lein codox
