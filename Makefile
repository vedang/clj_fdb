.PHONY: test
compileclj:
	@echo "\nRecompiling Clojure files"
	lein do clean, deps, compile

startclj:
	@echo "\nStarting a Clojure Repl"
	lein trampoline repl :headless

clj: compileclj startclj

test: compileclj
	@echo "\nRunning all tests"
	lein test
