var fs = require('fs');

process.on('message', function(m) {
    if(m.command==="upload") {
        console.log("Filepath: " + m.filepath);
        console.log("Filename: " + m.filename);
        console.log("Part: " + m.part);
        console.log("Size: " + m.size);
        
        fs.open(m.filepath, 'r', function(err, fd){
            if(err) {
                process.send({
                    "command": "error",
                    "message": "eh... something went wrong opening the file"
                });
            }
            
            var buf = new Buffer(m.size);  
            var position = m.part * m.size;
            fs.read(fd, buf, 0, m.size, position, function(err, bytesRead, buffer) {
                if(err) {
                    process.end({
                        "command": "error",
                        "message": "eh... someting went wrong reading the file"
                    });
                }
                
                
                var data = buffer.slice(0, bytesRead);  // in case it's the last chunk
                // TODO: Send the data to S3!
                
                // It all worked!
                process.send({
                    "command": "complete",
                    "part": m.part,
                    "etag": "this_is_an_etag_from_s3_for_part_" + m.part,
                });
            });
            
        });
    }
});
