const fs = require("fs");
const path = require("path");

// Read all files in the directory
function processDirectory(directoryPath) {
  fs.readdir(directoryPath, { withFileTypes: true }, (err, files) => {
    if (err) {
      return console.error("Unable to scan directory: " + err);
    }

    files.forEach((file) => {
      const filePath = path.join(directoryPath, file.name);

      if (file.isDirectory()) {
        // Recursively process subdirectory
        processDirectory(filePath);
      } else if (file.isFile()) {
        // Read each file
        fs.readFile(filePath, "utf8", (err, data) => {
          if (err) {
            return console.error("Unable to read file: " + err);
          }

          // Process the file content
          const processedData = processContents(data);

          // Write the processed content back to the file
          fs.writeFile(filePath, processedData, "utf8", (err) => {
            if (err) {
              return console.error("Unable to write file: " + err);
            }
            console.log(`Processed file: ${filePath}`);
          });
        });
      }
    });
  });
}

function processContents(contents) {
  // This changes the parameters section to put the parameter description on
  // the same line as the bullet with the parameter name and type.
  return contents.replace(/(## Parameters[\s\S]*?)(?=##|$)/g, (match) => {
    let lines = match
      .split("\n")
      .map((line) => line.trim())

      .filter((line) => line !== "")
      .map((line) => {
        if (line.startsWith("##")) {
          return line;
        } else if (line.startsWith("•")) {
          return "\n*" + line.substring(1);
        } else {
          return "    " + line;
        }
      });
    return lines.join("\n") + "\n\n";
  });
}

// Start processing from the root directory
processDirectory("../docs/src/js");
