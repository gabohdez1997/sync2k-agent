const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, '..', 'profit-web', 'src', 'routes', 'dashboard', 'billing', '+page.svelte');
if (fs.existsSync(filePath)) {
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split('\n');
  lines.forEach((line, idx) => {
    if (line.includes('pedido') || line.includes('Pedido') || line.includes('co_ven') || line.includes('vendedor') || line.includes('Vendedor')) {
      console.log(`Line ${idx + 1}: ${line.trim()}`);
    }
  });
} else {
  console.log('File not found:', filePath);
}
