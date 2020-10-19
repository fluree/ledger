# Changelog

## 0.15.2
- Update Admin UI [FC-647]

## 0.15.1
- Fixed prettyPrint bug, where variable names were inaccurate

## 0.15.0
- Storage :version (in raft state) is not backwards compatible. If someone upgrades, then tries an old version, should shutdown with error message [FC-57]
- Ability to create a snapshot with no history [FC-116]
- Delete ledger- garbage collect to delete files [FC-136]
- Query with sync-to block [FC-182]
- Handle remove-from-idx feature [FC-229]
- Endpoint to see all available snapshots [FC-275]
- FullText index - add lock [FC-312]
- FullText index - sync on node start-up [FC-315]
- Modify fluree release script to also allow 'latest' of a version for reference in docs, marketing [FC-350]
- JavaScript Libraries - Handle expiration of token(s) on cached db queries [FC-365]
- JavaScript Libraries - Password Auth: Implement the createUser option [FC-366]
- Add open-api setting to network state endpoint (nw-state) [FC-406]
- Handle index "underflow" situation, see fluree.db.ledger.indexing or ES-33 [FC-417]
- Mutable: snapshot and snapshot-no-history test with large ledger [FC-420]
- Mutable: Switch to _tx/hash, not _block/hash [FC-422]
- Mutable: Block-version use url-safe separator (not :) [FC-423]
- Mutable: Reindex not working properly for indexed predicates [FC-425]
- java.lang.IndexOutOfBoundsException: start greater than end in subrange [FC-438]
- Full-text indexes should not be supported in-memory [FC-513]
- If fdb-consensus-type is in-memory, file-storage should automatically default to in-memory [FC-514]
- Providing a private key in a .txt fails [FC-523]
- Changing fdb-group-log-history does not change the number of historic logs that are kept [FC-532]
- pretty-print option in queries should be prettyPrint to better support JS. Probably support both formats but deprecate pretty-print [FC-550]
- show-auth option in history query should be showAuth to better support JS. Probably support both formats but deprecate show-auth [FC-551]
- Fix issue where transactions consistently timeout for cljs, on-demand [FC-554]

# 0.14.0
- Promoted 0.13.6 to stable [FC-437]

## 0.13.6
- Admin UI - Fix Spec Doc bug [ES-32]
- Admin UI - Change to new fluree-icon for favicon

## 0.13.5
<<<<<<< HEAD

- Admin UI - Display error message when snapshot request fails [FC-476]


=======
- Admin UI - Display error message when snapshot request fails [FC-476]

>>>>>>> v0.17
## 0.13.4

- Throw port in use, error on start-up, instead of stacktrace [FC-334]
- Fluree to come with logback for ease of setup, logback file automatically set. + Admin UI URL is command and click [FC-357, FC-335]
- better error when unique predicate already exists [FC-356]
- Version number not updating in Raft state. [FC-259] 


## 0.13.3

- HttpApi Password Auth: Implement the createUser option [FC-367]
- Admin UI - Explore Page - Block 1 Needs Special Logic [FC-385]
- Admin UI - if a db is in status initialize, the account page should still load [FC-375]
- Admin UI - Explore Page - Block 1 Needs Special Logic [FC-385]
- Admin UI - Replace Fluree icon on admin UI [FC-394]
- Admin UI - Revert changes on Account since ledger-stats is anon [FC-410]
- Admin UI - Update Explore Ledger Page icon [FC-411]
- Admin UI - Display error/warning when ledger-stats returns database is not available (400) [FC-412]

## 0.13.2

- Improvements to Admin UI handling issues when server has closed-api [FC-261]


## 0.13.1

- Allow anonymous access to ledger-stats end-point [FC-388]
- (cljs) Password Auth return ExceptionInfo as promise "reject" [FC-198]


## 0.13.0

- CLI tool for RAFT, key management, view memory stats, etc [FC-204]
- Provide apis for ledger-info and ledger-stats [FC-221]
- Create ability to export Fluree ledger into RDF-XML and TTL [FC-230]
- fluree.db.transactor: Modify reference to Java v8 in code as minimum required version [FC-242]
- Short-circuit flake filtering across web socket when root auth is used (open-api=true) [FC-294]
- HTTP-signatures need to work in cljs/javascript natively, remove host from sig [FC-303]
- Error messages not propagated through the web socket (:db/invalid-query, :db/invalid-auth) [FC-330]
- Password Auth: Sync api logic with http-api [FC-358]
- AdminUI: Provide ability to scroll through Collections on the Explore page (new) [FC-260]
- AdminUI - when create first _user/_auth/_role/_rule the txn results don't stay in the editor [FC-207]
- AdminUI: Add Current Block Number, Ledger Size (kb, #flakes) to each Ledger/Database on the Account page [FC-264]
- AdminUI: Provide ability to scroll through Collections on the Explore page (new) [FC-265]
- AdminUI: Provide ability to scroll through Blocks for a Ledger on the Explore page (new) [FC-267]
- AdminUI: database name truncations should be longer than they are. They cut off unnecessarily early--plenty of UI space for more text [FC-325]
- Admin UI: Explore Ledger - Recommended Changes for 0.13 Release [FC-351]

## 0.12.4
- If vector wrapped in a vector gets passed to hash-set, then apply hash-set to the interior vector.

## 0.12.3
- Deleting a subject that includes already deleted predicates, simply retracts those predicates, rather than throwing an error. 

## 0.12.2
- Ensure queries skip over any empty index nodes, rather than attempting to read them [ES-33]

## 0.12.1
- UI: Fix signing command issue in closed-API [ES-31]

## 0.12.0
- React Wrapper: Improve Time Travel Widget [FC-89]
- Fix Queries where last subject is split across multiple nodes, only includes part of the subject [FC-166]
- Verify that large transactions (up to 2MB) are properly handled, without crashing [FC-175]
- Fluree-React Wrapper not receiving updated blocks when ledger updated [FC-234]
- Fix block range query was not responding [FC-240]
- ISO-8601 strings not accepted in /block queries [FC-241]
- History query not working when block not provided [FC-268]
- Flureeworker ignores block when specified in a query [FC-273]
- Flureedb transaction ids do not match expected values/formats [FC-274]

## 0.11.7

- Ensure history qurey returns proper response without block having to be specified.

## 0.11.6

- Ensure private key properly decrypted in JWT token.

## 0.11.5 
- Fix block range query was not responding 

## 0.11.4

- Ensure nested transaction doesn't create duplicate tempids. This appears to have been overwritten. 

## 0.11.3

- Ensure encryption-at-rest for enterprise customers properly writing to file [EC-28]

## 0.11.2

- Fix issue with nested transactions (same as 0.10.7, but never merged).

## 0.11.1 

- Ensure fdb-api-open was being read properly from settings.

## 0.11.0

- Support Password Authentication for Private Key Generation [FC-13]
- Archive databases + create database from archive [FC-30]
- Check if in 0.9.6, setting two predicates to null retracts both of them, or just the first one. [FC-69]
- Make db names a-z0-9-, including upgrade script [FC-70]
- Deletion of multiple predicates at once was failing in certain circumstances [FC-75]
- Removed fdb-sendgrid-auth from server settings [FC-86]
- Support Delete Ledger action [FC-90]
- Add permissioned flake filtering to index and block retrieval, along with block push via websockets [FC-93]
- Create API to get Password-Auth JWT token [FC-95]
- JavaScript-Compiled APIs: Signed queries should be submitted as commands to the server [FC-148]
- Incorporate Password-Auth JWT token in cljs api library [FC-150]
- _tx/tempids has an empty map "{}" in tx flakes when no tempids. Should not be included in block at all if empty. [FC-152]
- Create api to renew JWT tokens [FC-158]
- Check for JWT tokens in http API and sign requests as needed [FC-162]
- Data version upgrade triggers with new instance [FC-167]
- Create Build constants namespace/file [FC-168]
- Fluree React Wrapper: Wire-Up DB Connection Event [FC-170]
- Raft DB Sync Inconsistency [FC-174]
- Test that large transactions (up to 2MB) are properly handled, without crashing. [FC-175]
- Lagging back-slash causes 404 [FC-178]
- Change "archive" naming to "snapshot" [FC-185]
- Create database from snapshot should throw error if snapshot file incorrect. [FC-212]

## 0.10.9

- Ensure nested transaction doesn't create duplicate tempids.

## 0.10.8

- Ensure query that retrieves signed command results goes to the correct endpoint. 

## 0.10.7

- Fix bug where nested transactions using subject ids failed.

## 0.10.6

- Allow queries in smart functions to be a string

## 0.10.5

- Add DEBUG logging for smart function (back in?)
- Switch the name for nil? and empty? smart functions (names were flipped)

## 0.10.4

- Bump React version in UI to re-enable Schema page. 

## 0.10.3

- Ensure fdb-api-open setting is deactivated when fdb-api-open is set to false
- Add DEBUG logging for http-signatures (when fdb-api-open = true only)

## 0.10.2

- In the UI, allow 'host' to be viewed and changed when signing queries 
- Fix issue with unique two-tuples not being recognized as references 

## 0.10.1

- Ensure JSON-type predicates are not mistaken for nested transactions
- Add DEBUG logging for smart functions

## 0.10.0

- Switch to semantic version (previous version 0.9.5 is 0.10.0)
- Raft checks every block and log file when starting up or server rejoins the network.
- Top-level sort capabilities in GraphQL and FlureeQL
- Ensure settings are determined in order of precedence: Environmental Variables, Java Property Flags (-Dxxxx), .properties file.


## 0.9.5-PREVIEW2

- Raft snapshots only happening at reboots, not purging
- Multi-cardinality recursion fix
- Multi-cardinality delete transaction fix

