const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, 'routes', 'cotizaciones.js');
if (fs.existsSync(filePath)) {
  const content = fs.readFileSync(filePath, 'utf8');
  const selectIdx = content.indexOf('request.query(`');
  if (selectIdx !== -1) {
    const endIdx = content.indexOf('`)', selectIdx);
    console.log(content.substring(selectIdx, endIdx + 2));
  } else {
    console.log('SQL query not found');
  }
} else {
  console.log('File not found:', filePath);
}
