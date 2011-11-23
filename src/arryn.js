var cp = require('child_process');
var fs = require('fs');
var os = require('os');

// CONSTANTS
var MINIMUM_PART_SIZE = 5 * 1024 * 1024;    // S3 requires each part to be at least 5MB
var MAXIMUM_PARTS = 10000;                  // S3 "only" allows 10k parts

var filepath = process.argv[2];
// get the last part from the filepath
var filename = filepath.substring(filepath.lastIndexOf("/")+1);  
var aws_access_key = "FILL ME IN";
var aws_secret_key = "FILL ME IN";

console.log("Uploading file '" + filepath + "'...");

/*
 * Fork a child node process for every CPU on the system
 */
var n_procs = os.cpus().length;
var procs = [n_procs];
console.log("Detected " + n_procs + " CPUs, spawning " + n_procs + " threads.");
for(var i=0;i<n_procs;i++) {
    procs[i] = cp.fork(__dirname + "/part.js");
}

/* 
 * Check out the file we are uploading and split it into parts
 */
fs.stat(filepath, function(err, stats){
    if(err) {
        console.log(err);   // TODO: Better error message
        quit();
    }
    var filesize = stats.size;
    if(filesize < MINIMUM_PART_SIZE) {
        console.log("File is too small for multipart upload");
        quit();
    }
    
    // Calculate the number of "chunks" and the "chunk size" per chunk.
    var num_chunks = MAXIMUM_PARTS;
    var chunk_size = MINIMUM_PART_SIZE;
    
    if(filesize > MAXIMUM_PARTS * MINIMUM_PART_SIZE) {
        // filesize is bigger than 10,000 * 5MB (48.828125 GB) then we use chunks bigger than 5MB
        chunk_size = Math.ceil(filesize * 1.0 / MAXIMUM_PARTS);
    } else {
        // filesize is smaller than 10,000 * 5MB (48.828125 GB), use 5MB chunks
        num_chunks = Math.ceil(filesize * 1.0 / MINIMUM_PART_SIZE);
    }
    
    console.log("Splitting file into " + num_chunks + " parts of size " + chunk_size + " bytes.");
    
    // Initiate an Amazon S3 multipart upload
    initiate_multipart_upload(function() {
        // Offload each part to upload to a child process until all the parts are done
        upload_parts(num_chunks, chunk_size);
    });
});

var upload_parts = function(num_chunks, chunk_size) {
    var registry = [num_chunks];
    for(i=0;i<num_chunks;i++) {
        registry[i] = {
            "state": "pending",     // possible states: pending, started, complete, error
            "etag": null
        };
    }
    
    /* Helper method to set up the handler for a proc inside a loop */
    var set_proc_handler = function(proc) {
        proc.on('message', function(m){
            // Check the message for "upload complete" vs "error"
            if(m.command==="complete") {
                // Upload complete, save the etag and upload the next part
                registry[m.part]['state'] = "complete";
                registry[m.part]['etag'] = m.etag;
            
                // Check to see if the registry is done
                for(var i=0, complete=true; i<registry.length; i++) {
                    if(registry[i]['state']!=="complete") {
                        complete=false;
                        break;
                    }
                }
                if(complete) {
                    // TODO: Finish multipart upload
                    // Complete the multipart upload
                    complete_multipart_upload();
                }
            } else {
                console.log("OH NOES - Error upload a part!");
                registry[m.part]['state']="error";
            }
        });
    }
    
    // Set up the event handler for each process
    for(var i=0;i<n_procs;i++) {
        set_proc_handler(procs[i]);
    }
  
    // create an e-tag registry for all the parts
    
    // Upload each part with a different process
    for(var i=0; i<num_chunks; i++) {
        var proc = procs[i%n_procs];
        upload_part(proc, registry, i, chunk_size);
    }
};

var upload_part = function(proc, registry, part, size) {
    console.log("Initiating upload of part " + part);
    registry[part].state="uploading";
    proc.send({
        'command': 'upload',
        'filepath': filepath,
        'filename': filename,
        'part': part,
        'size': size
    });
}

/* Initiate a multipart upload on Amazon S3 */
var initiate_multipart_upload = function(callback){
    // TODO - Start the upload then fire the callback callback
    callback();
};

/* Complete a multipart upload on Amazon S3 */
var complete_multipart_upload = function() {
    // TODO - Finish the multipart upload and print out a message
    console.log("Upload complete!");
    quit();
};

/*
 * Quit the current process making sure to kill all the child processes we spawned at the start
 */
var quit = function() {
    for(i=0;i<n_procs;i++) {
        procs[i].kill();
    }
    process.kill(process.pid);
}

process.on('exit', function(){
    quit();
});

