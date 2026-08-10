const apiKey = '7332bd8f617d4671c748cd7cd9fa413386256ae06b0faeec2f1e727152b2cd22';
const url = 'http://localhost:3001/api/v1/articulos?limit=1';

fetch(url, {
    headers: {
        'x-api-key': apiKey,
        'Content-Type': 'application/json'
    }
})
.then(res => res.json())
.then(data => {
    console.log(JSON.stringify(data, null, 2));
    process.exit(0);
})
.catch(err => {
    console.error(err);
    process.exit(1);
});
