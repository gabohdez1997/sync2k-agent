const fs = require('fs');
const path = require('path');

function searchSvelte(dir) {
  const files = fs.readdirSync(dir);
  files.forEach(file => {
    const fullPath = path.join(dir, file);
    const stat = fs.statSync(fullPath);
    if (stat.isDirectory()) {
      if (file !== 'node_modules' && file !== '.git') {
        searchSvelte(fullPath);
      }
    } else if (file.endsWith('.svelte') || file.endsWith('.ts')) {
      const content = fs.readFileSync(fullPath, 'utf8');
      if (content.toLowerCase().includes('anular') || content.toLowerCase().includes('void')) {
        console.log('Found file with anular/void:', fullPath);
        const lines = content.split('\n');
        lines.forEach((line, index) => {
          if (line.toLowerCase().includes('anular') || line.toLowerCase().includes('void') || line.toLowerCase().includes('confirm') || line.toLowerCase().includes('prompt')) {
            console.log(`Line ${index + 1}: ${line.trim()}`);
          }
        });
      }
    }
  });
}

searchSvelte(path.join(__dirname, '..', 'profit-web', 'src', 'routes'));
