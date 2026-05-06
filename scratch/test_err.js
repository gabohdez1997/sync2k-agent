const sql = require('mssql');
try {
    throw new sql.RequestError('Some SQL Error');
} catch (err) {
    console.log("MESSAGE:", err.message);
    console.log("STRING:", String(err));
    console.log("JSON:", JSON.stringify(err, Object.getOwnPropertyNames(err)));
}
