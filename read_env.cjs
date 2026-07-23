const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, '..', 'profit-web', '.env');
if (fs.existsSync(filePath)) {
  console.log(fs.readFileSync(filePath, 'utf8'));
} else {
  console.log('File not found:', filePath);
}
