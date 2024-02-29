# Working with remote socket REPL
Optionally, Fluree can be started with a Remote REPL socket open for debugging purpose.

The ./fluree_start.sh script has an option that automatically enables this feature.

When starting with ./fluree.start.sh, simply add the option:
```bash
./fluree_start.sh -repl-port=5555
```
This will start Fluree with a remote REPL socket open on port 5555.
