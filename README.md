# Concurrent Transaction Processing Using MongoDB

This parallel program performs atomic transactions on MongoDB
documents using fine-grained record locking.  Synchronization is
based on Tera MTA-style Full/Empty memory semantics and access
primitives (readFE, writeEF), and the state is stored in the
{full_empty:'empty'} property (absence of the property means full).

The program's master task first generates the documents which will
be updated and stores them in the db.accounts collection.  Next,
the master task forks the threads that consume transactions, then
the master thread begins generating transaction documents that are
stored in the db.transactions collection.  Each transaction
updates a random number of random records.  The master task blocks
until all transaction processing tasks exit, then the master task
performs final self-consistency and correctness checks.


## Usage
```
usage:  mongo parallel_trans.js
```


## Arguments / Options
Execution arguments are all configured in the cmdArgs object below:

```
cmdArgs = {
    nTasks           : 4,         //  # of concurrent threads/clients to start
    initDB           : true,      //  Re-initialize the databases
    nAccounts        : 10000,     //  Number of records to create
    nTransactions    : 1000,       //  # of transactions to perform
    minAcctsPerTrans : 1,         //  # of records updated per transactions
    maxAcctsPerTrans : 3,         //  # of records updated per transactions
    latencyLimit     : 1000,      //  ms of latency before long latency trap on AMOs
    taskIdleSleep    : 50,        //  ms to yield before checking for more work
    amoExecKind      : 'client',  //  client, ss_eval, ss_runcmd
    backoffRate      : 1.1,       //  Decay rate for F/E retries
    verbose          : true,      //  Print progress messages
    acctID           : -1,        //  Key to find this document -- Do not remove
    doTransJournaling  : false,   //  Perform journaling 
    verifyTransUpdates : false,   // Verify updates to transaction state
    acctRangeForTransactions : 0.50  //  Restrict updates to this % of acctIDs
};
```
