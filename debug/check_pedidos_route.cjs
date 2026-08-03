const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, 'routes', 'pedidos.js');
if (fs.existsSync(filePath)) {
  const content = fs.readFileSync(filePath, 'utf8');
  console.log(content.substring(0, 1500)); // print the first 1500 chars
} else {
  console.log('File not found:', filePath);
}
