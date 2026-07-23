const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, '..', 'profit-web', 'src', 'routes', 'dashboard', 'billing', '+page.svelte');
if (fs.existsSync(filePath)) {
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split('\n');
  lines.forEach((line, idx) => {
    if (line.includes('mont_cob') || line.includes('monto_cob') || line.includes('montoCobrar') || line.includes('tasa') || line.includes('redondeo')) {
      console.log(`Line ${idx + 1}: ${line.trim().substring(0, 100)}`);
    }
  });
} else {
  console.log('File not found:', filePath);
}
