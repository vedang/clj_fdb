.PHONY: test
test:
# -U option to force fetch SNAPSHOTS
# Ref: https://github.com/technomancy/leiningen/blob/master/src/leiningen/deps.clj#L112
	@echo "\nRunning all tests"
	lein do clean, -U deps, compile, test

compileclj:
	@echo "\nRecompiling Clojure files"
	lein do clean, deps, compile

startclj:
	@echo "\nStarting a Clojure Repl"
	lein trampoline repl :headless

clj: compileclj startclj
