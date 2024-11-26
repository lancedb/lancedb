const fs = require('fs');
const path = require('path');


// Go through each file

// When find each markdown header with Parameters

// Find each section after a bullet "•", and indent all the lines without bullet
// by 4 spaces

// Write out the files.
const directoryPath = '/Users/willjones/Documents/lancedb/nodejs/docs';

// Read all files in the directory
fs.readdir(directoryPath, (err, files) => {
    if (err) {
        return console.log('Unable to scan directory: ' + err);
    }

    files.forEach((file) => {
        const filePath = path.join(directoryPath, file);

        // Read each file
        fs.readFile(filePath, 'utf8', (err, data) => {
            if (err) {
                return console.log('Unable to read file: ' + err);
            }

            // Process the file content
            const processedData = data.replace(/(## Parameters[\s\S]*?)(?=##|$)/g, (match) => {
                return match.replace(/(\n• [^\n]*\n)([^\n•])/g, (bulletMatch, bullet, text) => {
                    return bullet + text.replace(/^/gm, '    ');
                });
            });

            // Write the processed content back to the file
            fs.writeFile(filePath, processedData, 'utf8', (err) => {
                if (err) {
                    return console.log('Unable to write file: ' + err);
                }
                console.log(`Processed file: ${file}`);
            });
        });
    });
});