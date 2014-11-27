/*-------------------------------------------------------------------------+
 |               Parallel Transactional Processor for MongoDB              |
 +-------------------------------------------------------------------------+
 |  Written by Jace A Mogill  |  mogill@synsem.com  |  www.synsem.com      |
 |                  Copyright 2012 Synthetic Semantics                     |
 | ***  ***     AS IS    --    NO WARRANTY    --    MIT LICENSE    *** *** |
 +-------------------------------------------------------------------------+
 |  This parallel program performs atomic transactions on MongoDB          |
 |  documents using fine-grained record locking.  Synchronization is       |
 |  based on Tera MTA-style Full/Empty memory semantics and access         |
 |  primitives (readFE, writeEF), and the state is stored in the           |
 |  {full_empty:'empty'} property (absence of the property means full).    |
 |                                                                         |
 |  The program's master task first generates the documents which will     |
 |  be updated and stores them in the db.accounts collection.  Next,       |
 |  the master task forks the threads that consume transactions, then      |
 |  the master thread begins generating transaction documents that are     |
 |  stored in the db.transactions collection.  Each transaction            |
 |  updates a random number of random records.  The master task blocks     |
 |  until all transaction processing tasks exit, then the master task      |
 |  performs final self-consistency and correctness checks.                |
 |                                                                         |
 |  Execution arguments are all configured in the cmdArgs object below.    |
 |                                                                         |
 |  usage:  mongo parallel_trans.js                                        |
 |                                                                         |
 |  Note: Server side AMO execution (amoExecKind = ss_eval or ss_runcmd)   |
 |        produces intermittent errors with multiple tasks, but works with |
 |        one client thread.  Possible misuse of API?                      |
 +-------------------------------------------------------------------------*/
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
    verbose          : true,	  //  Print progress messages
    acctID           : -1,        //  Key to find this document -- Do not remove
    doTransJournaling  : false,	  //  Perform journaling 
    verifyTransUpdates : false,   // Verify updates to transaction state
    acctRangeForTransactions : 0.50  //  Restrict updates to this % of acctIDs
};



/*-------------------------------------------------------------------------+
 |  Generate a random integer within a range (inclusive) from 'low' to 'high'
 +-------------------------------------------------------------------------*/
function randomInRange(low, high) {
    return( Math.floor((Math.random() * (high - low)) + low) );
}



/*-------------------------------------------------------------------------+
 |  Atomically read a DB object when marked {full_empty:'full'} and
 |  mark the DB copy 'empty'.  If empty, retry until the latency limit
 |  is reached.  Returns the DB object without the full_empty property.
 |  Optional argument userUpdates includes other DB update command(s)
 +-------------------------------------------------------------------------*/
function readFE( key, collectionName, userUpdates ) {
    var collection = eval(collectionName);
    var sleepDuration = 1;
    key.full_empty = { $exists : false };
    if(!userUpdates) userUpdates = new Object();
    if(!userUpdates.$set) userUpdates.$set = new Object();
    userUpdates.$set["full_empty"] = 'empty';
    while( true ) {
	var result = collection.findAndModify( 
	    { query : key,  update : userUpdates,  new : true } );
	if(result  &&  result.full_empty == 'empty') {
	    delete(result.full_empty);
	    return(result);
	} else {
	    nReadFERetries++;
	    sleep(sleepDuration);
            sleepDuration *= cmdArgs.backoffRate;
            if(sleepDuration > cmdArgs.latencyLimit)
		throw new Error("readFE: long latency trap " + sleepDuration);
	}
    }
}



/*-------------------------------------------------------------------------+
 |  Atomically write 'obj' to a collection when the DB copy is marked
 |  {full_empty:'empty'}, and mark full by deleting the full_empty property
 |  from the DB record.  Retry until the latency limit is exceeded.
 +-------------------------------------------------------------------------*/
function writeEF( key, obj, collectionName ) {
    var collection = eval(collectionName);
    var sleepDuration = 1;
    key.full_empty = 'empty';
    delete(obj.full_empty);
    while( true ) {
	var result = collection.findAndModify( 
	    { query : key,  update : obj,  new : true } );
        if( result  &&  !result.full_empty ) { 
	    return;
	} else {
	    nWriteEFRetries++;
            sleep(sleepDuration);
            sleepDuration *= cmdArgs.backoffRate;
            if(sleepDuration > cmdArgs.latencyLimit)
                throw new Error("writeEF: long latency trap " + sleepDuration);
	}
    }
}



/*-------------------------------------------------------------------------+
 |  Initiate one transaction by aquiring all the requested
 |  account DB objects, returning them as an array of objects.
 +-------------------------------------------------------------------------*/
function transactionAquire(transaction) {
    var journalCmd = (cmdArgs.doTransJournaling) ? 
	{$push : {pendingTransactions:transaction.transID}} : null;

    //  Always aquire locks in deterministic order
    transaction.records.sort( function(a, b) { return a - b } );

    var recs = new Array();
    transaction.records.forEach( function( transAcctID ) {
	switch(cmdArgs.amoExecKind) {
	case 'client':
	    var origData = readFE( {'acctID' : transAcctID}, db.accounts, journalCmd );
	    break;
	case 'ss_eval':
	    var origData = db.eval( readFE, {'acctID' : transAcctID}, 
				    'db.accounts', journalCmd );
	    break;
	case 'ss_runcmd':
	    var origData = db.runCommand( {
		$eval  : readFE,  nolock : false, 
		args   : [ {'acctID' : transAcctID}, 'db.accounts', journalCmd ] } 
					 ).retval;
	    break;
	default:
            assert("Unknown amoExecKind: " + cmdArgs.amoExecKind);
	};

	assert(origData, "Results from server-side execution is missing");
	recs.push( origData );
	
	//  Journal progress to the transaction halfway completed point
	if(cmdArgs.doTransJournaling) {
	    if(cmdArgs.verifyTransUpdates) {
		transaction = db.transactions.findAndModify( {
		    query  : transaction,
		    update : { $set : {'state' : 'aquired'} },
		    'new'  : true } );
		assert(transaction.state == 'aquired', 
		       "missing pending transaction to update to aquired");
	    } else {
		transaction.state = 'aquired';
		db.transactions.save(transaction);
	    };
	};
    } );
    return[recs, transaction];
}



/*-------------------------------------------------------------------------+
 |  Atomically commit a pending transaction by writing the array of records 
 |  back to the DB.
 +-------------------------------------------------------------------------*/
function transactionCommit(transaction, recs) {
    recs.reverse();  // Release records in the reverse order they were aquired

    recs.forEach( function( rec ) {
	// Do journaling related bookkeeping and consistency checks
	if(cmdArgs.doTransJournaling) {
	    var transID = rec.pendingTransactions.pop();
	    assert( (transID == transaction.transID  &&
		     rec.pendingTransactions.length == 0 ),
		    "There should not be concurrent active transactions for this record" );
	}

	//  Write the records back to the database, marking them full
	switch(cmdArgs.amoExecKind) {
	case 'client':
	    writeEF({'acctID':rec.acctID }, rec, db.accounts);  break;
	case 'ss_eval':
	    db.eval( writeEF, {_id:rec._id}, rec, 'db.accounts' );  break;
	case 'ss_runcmd':
	    db.runCommand( { $eval: writeEF,  nolock : false ,
			     args: [{_id : rec._id}, rec, 'db.accounts'] } );
	    break;
	default:
	    assert("Unknown amoExecKind: " + cmdArgs.amoExecKind);
	};
    } );

    //  Journal the transaction as completed
    if(cmdArgs.doTransJournaling) {
	if(cmdArgs.verifyTransUpdates) {
	    transaction = db.transactions.findAndModify( {
		query  : transaction,
		update : { $set : {'state' : 'completed'} },
		'new'  : true } );
	    assert(transaction.state == 'completed', 
		   "missing pending transaction to update to aquired");
	} else {
	    transaction.state = 'completed';
	    db.transactions.save(transaction);
	}
    }
}




/*-------------------------------------------------------------------------+
 |  Initialize the database and server side stored functions
 +-------------------------------------------------------------------------*/
function initDB() {
    print("Resetting Databases...");
     // db.system.js.remove();
    db.system.js.save({ "_id" : "readFE", "value" : readFE });
    db.system.js.save({ "_id" : "writeEF", "value" : writeEF });
    db.system.js.save({ "_id" : "randomInRange", "value" : randomInRange });
    db.transactions.drop();
    db.accounts.drop();
    var timerStart = new Date();
    for(var recordN = 0;  recordN < cmdArgs.nAccounts;  recordN++) {
	if(recordN % 50000 == 0) print("Intialized record "+recordN);
	var rand = randomInRange(0, 1000);
	totalBalances += rand;
        db.accounts.insert({ 'pendingTransactions' : [ ],
			     'acctID'  : recordN, 'count' : 0, 
			     'balance' : rand });
    }
    var timerStop = new Date();
    db.transactions.ensureIndex({'transID' : 1});
    db.accounts.ensureIndex({'acctID' : 1});
    print("Inserted and indexed "+ cmdArgs.nAccounts + " documents, " +
          cmdArgs.nAccounts/((timerStop-timerStart)/1000) +"/sec");
}





/*-------------------------------------------------------------------------+
 |  Consume transactions and process them until there are no 'initial'
 |  transactions and there is a 'shutdown' command.  Transaction FSM:
 |     initial +> pending -> aquired +> completed
 |             |                     |
 |             +---------------------+  (Skip when not journaling)
 +-------------------------------------------------------------------------*/
function doTransactions() {
    var nTransDone = 0;
    var nRecsModified = 0;
    var shutdown = false;

    while(!shutdown) {
	//  Find a transaction to perform
	var transaction = db.transactions.findAndModify( {
	    query  : {'transID' : {$gte : 0}, 'state':'initial'},
	    update : { $set : {'state' : cmdArgs.doTransJournaling ? 'pending' : 'completed' } },
	    'new'  : true } );
	if(transaction == undefined) {
	    //  There were no 'initial' transactions, check for shutdown
	    //  If no 'initial' transactions or shutdown, wait & check again
	    var bcastCmd = db.transactions.findOne( {'transID' : {$lt : 0}} );
	    if(bcastCmd  &&  bcastCmd.broadcast == 'shutdown') {
		shutdown = true;
	    } else {
		sleep(cmdArgs.taskIdleSleep);
	    }
	} else {
	    //  This transaction is presently pending, begin processing by
	    //  initiating the transaction
	    [recs, transaction]  = transactionAquire(transaction);

	    //  Perform artibtrary operations on the locked records. These
	    //  operations compute checksums verified at the experiment's end.
	    var sum = 0;
	    recs.forEach( function(rec) { sum += rec.balance; } );
	    var avg = sum/recs.length;
	    recs.forEach( function(rec) { 
		rec.balance = avg;  
		rec.count++;
	    } );

	    //  Write the records back to the database and retire the transaction
	    transactionCommit(transaction, recs);

	    //  Bookkeeping and diagnostics
	    nTransDone++;
	    nRecsModified += recs.length;
	    if( cmdArgs.verbose  &&  transaction.transID % 500 == 0 ) {
		var timerEnd = new Date();
		var timerElapsed = timerEnd - timerStart;
		print("Transaction " + transaction.transID + " performed by Task " 
		      + myID + "  Rate: " + transaction.transID  / (timerElapsed/1000) +
		     " transactions/sec");
	    }
	}
    }
    print("Exiting: Task " + myID + " completed " + nTransDone + 
	  " transactions on " + nRecsModified + " records.  #Retries: " + 
	  nReadFERetries + "r " + nWriteEFRetries + "w");
}



/*-------------------------------------------------------------------------+
 |  Perform self-consistency checks, validate checksums, etc.
 +-------------------------------------------------------------------------*/
function selfCheck() {
    var nErrors = 0;
    if( db.accounts.find({'full_empty' : 'empty', 
			  'count':{$gte : 0}} ).count() > 0 ) {
	print("ERROR: Some records still have full_empty marked empty"); 
	nErrors++;
    }

    var sumBalance = 0;
    var sumAMOs = 0;
    var nRecs = 0;
    db.accounts.find({balance : {$gte : 0}}).forEach( function(rec) {
	sumAMOs += rec.count;
	sumBalance += rec.balance;
	nRecs++;
	if(rec.pendingTransactions.length > 0) {
	    print("ERROR: Account record has live transaction");
	    nErrors++;
	}
    });

    if(sumAMOs != totalNRecsModified) {
	print("ERROR: # of AMOs is not correct " + sumAMOs + 
	      ", expected " + totalNRecsModified);
	nErrors++;
    } 

    if(Math.abs(sumBalance - totalBalances) >= 0.5) {  // Rounding error
	print("ERROR: Sum of all balances " + sumBalance + 
	      " different from expected " + totalBalances);
	nErrors++;
    } 

    // One non-completed event is the shutdown broadcast transaction
    if(db.transactions.find({'state' : {$ne : 'completed'}}).count() != 1) { 
	print("ERROR: Not all transations are completed");
	nErrors++;
    }

    print("selfCheck: " + nErrors + " errors");
}



/*-------------------------------------------------------------------------+
 |  Generate transactions, each involving a random number of random records.
 +-------------------------------------------------------------------------*/
function generateTransactions(nTransactions) {
    for(var transactionN = 0;  transactionN < nTransactions;  transactionN++) {
	var nRecs = randomInRange(cmdArgs.minAcctsPerTrans, cmdArgs.maxAcctsPerTrans);
	totalNRecsModified += nRecs;
	var randomAcctIDs = new Array();
	var maxAcctID = cmdArgs.acctRangeForTransactions * cmdArgs.nAccounts;

	//  Initial guess of random numbers, possibly generating duplicates
	for(var recn = 0;  recn < nRecs;  recn++)
	    randomAcctIDs.push( randomInRange(0, maxAcctID) );
	
	//  Make entires unique by replacing duplicates with new random guesses
	var duplicatesExist = true;
	while( duplicatesExist ) {
	    randomAcctIDs.sort( function(a, b) { return a - b } );
	    var prevID = randomAcctIDs[0];
	    var recn = 1;
	    while( recn < randomAcctIDs.length  &&  randomAcctIDs[recn] != prevID) {
		prevID = randomAcctIDs[recn];
		recn++;
	    }
	    if(recn < randomAcctIDs.length  &&  randomAcctIDs[recn] == prevID) {
		var rand = randomInRange(0, maxAcctID);
		randomAcctIDs[recn] = rand;
	    } else {
		duplicatesExist = false;
	    }
	}
	db.transactions.insert( { 'transID'    : transactionN,  
				  'state'      : 'initial',  
				  'records'    : randomAcctIDs } );
	if(cmdArgs.verbose  &&
	   transactionN % 1000 == 0) { print("Generating Transaction " + transactionN) };
    }
    //  Enqueue shutdown command AFTER all transactions have been created
    db.transactions.insert( { transID : -1,  broadcast : 'shutdown'} );
}




/*-------------------------------------------------------------------------+
 |                    B E G I N    P R O G R A M
 +-------------------------------------------------------------------------*/
var nReadFERetries     = 0;  //  # of read retries due to empty data
var nWriteEFRetries    = 0;  //  # of write retires due to full data
var totalBalances      = 0;  //  Sum of balances of all accounts
var totalNRecsModified = 0;  //  # of account record updates the experiment performs
var timerStart = new Date(); //  Time consumer tasks were started

if(typeof myID != 'undefined') { 
    // myID is already assigned, therefore this task was forked by the master
    doTransactions();
} else {
    // myID is not assigned, this is the master task's entry point
    if(cmdArgs.initDB) { initDB(); };
    printjson(cmdArgs);

    var taskJoins  = new Array();
    //  Start tranaction processing (consumer) tasks
    for( var taskn = 0;  taskn < cmdArgs.nTasks;  taskn++ ) {
	var join = startParallelShell("var myID = " + taskn +
				      "; load('parallel_trans.js')" );
	taskJoins.push(join);
    }

    timerStart = new Date();   //  (Re-)Start timer here when work is created
    generateTransactions(cmdArgs.nTransactions);

    // Barrier by joining every task 
    taskJoins.forEach(function (join) { join() });  
    var timerEnd = new Date();
    var timerElapsed = timerEnd - timerStart;

    selfCheck();
    var nDBOps = 
	cmdArgs.nTransactions +  // Transactions: init->pending or completed
	totalNRecsModified +     // Accounts: Aquire readFE
	(cmdArgs.doTransJournaling  ? cmdArgs.nTransactions : 0) + // Transactions: pending->aquired
	totalNRecsModified +     // Accounts: commit writeEF
	(cmdArgs.doTransJournaling  ? cmdArgs.nTransactions : 0); // Transactions: aquired->completed
    print("DB Ops Performed: " + nDBOps +  "    Elapsed: " + timerElapsed + "ms  " +
	  (nDBOps / (timerElapsed/1000)) + " DB ops/sec");
    print("Transaction Performance: " + (cmdArgs.nTransactions) / (timerElapsed/1000) + 
	  " trans/sec.");
}
